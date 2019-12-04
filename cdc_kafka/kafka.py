import collections
import csv
import datetime
import json
import logging
import os
import socket
import time
from typing import List, Dict, Tuple, Any, Callable, Union, Generator

import avro
import confluent_kafka.admin
import confluent_kafka.avro

from . import constants

logger = logging.getLogger(__name__)


class KafkaClient(object):
    __instance = None

    def __init__(self,
                 bootstrap_servers: str,
                 schema_registry_url: str,
                 kafka_timeout_seconds: int,
                 progress_topic_name: str,
                 capture_instance_name_resolver: Callable[[str], str],
                 extra_kafka_consumer_config: str,
                 extra_kafka_producer_config: str):

        if KafkaClient.__instance is not None:
            raise Exception('KafkaClient class should be used as a singleton.')

        self._kafka_timeout_seconds: int = kafka_timeout_seconds
        self._progress_topic_name: str = progress_topic_name
        self._capture_instance_name_resolver: Callable[[str], str] = capture_instance_name_resolver

        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': f'cdc_to_kafka_progress_check_{socket.getfqdn()}',
            'enable.partition.eof': True
        }
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': '50',
            'enable.idempotence': True,
            'enable.gapless.guarantee': True,
            'retry.backoff.ms': 250,
            'compression.codec': 'snappy'
        }
        admin_config = {
            'bootstrap.servers': bootstrap_servers
        }

        if extra_kafka_consumer_config:
            for extra in extra_kafka_consumer_config.split(';'):
                k, v = extra.split(':')
                consumer_config[k] = v

        if extra_kafka_producer_config:
            for extra in extra_kafka_producer_config.split(';'):
                k, v = extra.split(':')
                producer_config[k] = v

        logger.debug('Kafka consumer configuration: \n%s', json.dumps(consumer_config, indent=4))
        logger.debug('Kafka producer configuration: \n%s', json.dumps(producer_config, indent=4))
        logger.debug('Kafka admin client configuration: \n%s', json.dumps(admin_config, indent=4))

        consumer_config['error_cb'] = KafkaClient._raise_kafka_error
        consumer_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        consumer_config['logger'] = logger
        producer_config['error_cb'] = KafkaClient._raise_kafka_error
        producer_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        producer_config['logger'] = logger
        admin_config['error_cb'] = KafkaClient._raise_kafka_error
        admin_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        admin_config['logger'] = logger

        self._schema_registry = confluent_kafka.avro.CachedSchemaRegistryClient(schema_registry_url)
        self._avro_serializer = confluent_kafka.avro.MessageSerializer(self._schema_registry)
        self._consumer = confluent_kafka.Consumer(consumer_config)
        self._producer = confluent_kafka.Producer(producer_config)
        self._admin = confluent_kafka.admin.AdminClient(admin_config)
        self._avro_deserializers = {}
        self._commit_order_enforcer: Dict[Tuple, int] = {}

        self._cluster_metadata = None
        self.refresh_cluster_metadata()

        self._last_progress_commit_time: datetime.datetime = datetime.datetime.now()
        self._last_full_flush_time: datetime.datetime = datetime.datetime.now()
        self._successfully_delivered_messages_counter: int = 0
        self._produce_sequence: int = 0
        self._progress_messages_awaiting_commit: Dict[Tuple, Dict[int, Tuple]] = collections.defaultdict(dict)

        k_id, v_id = self.register_schemas(
            self._progress_topic_name, constants.PROGRESS_MESSAGE_AVRO_KEY_SCHEMA,
            constants.PROGRESS_MESSAGE_AVRO_VALUE_SCHEMA)
        self._progress_key_schema_id: int = k_id
        self._progress_value_schema_id: int = v_id

        KafkaClient.__instance = self

    def refresh_cluster_metadata(self) -> None:
        self._cluster_metadata = self._admin.list_topics(timeout=self._kafka_timeout_seconds)
        if self._cluster_metadata is None:
            raise Exception(f'Cluster metadata request to Kafka timed out')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        logger.info("Committing progress and cleaning up Kafka resources...")
        self._consumer.close()
        self.commit_progress(True)
        logger.info("Done.")

    def produce(self, topic: str, key: Dict[str, Any], key_schema_id: int, value: Union[None, Dict[str, Any]],
                value_schema_id: int) -> None:
        key_ser = self._avro_serializer.encode_record_with_schema_id(key_schema_id, key, True)
        if value is None:
            # deletion tombstone
            value_ser = None
        else:
            value_ser = self._avro_serializer.encode_record_with_schema_id(value_schema_id, value, False)
        # the callback function receives the binary-serialized payload, so instead of specifying it
        # directly as the delivery callback we wrap it in a lambda that also passes the original not-yet-
        # serialized key and value so that we don't have to re-deserialize it later:
        self._produce_sequence += 1
        seq = self._produce_sequence
        while True:
            try:
                self._producer.produce(
                    topic=topic, value=value_ser, key=key_ser,
                    callback=lambda err, msg: self._delivery_callback(err, msg, key, value, seq))
                break
            except BufferError:
                time.sleep(1)
                logger.debug('Sleeping due to Kafka producer buffer being full...')
                self.commit_progress()  # clear some space before retrying
            except Exception:
                logger.error('The following exception occurred producing to topic %s', topic)
                raise

    def reset_progress(self, topic_name: str, capture_instance_name: str) -> None:
        key = {
            'progress_kind': constants.CHANGE_ROWS_PROGRESS_KIND,
            'topic_name': topic_name,
            'capture_instance_name': capture_instance_name
        }
        self.produce(self._progress_topic_name, key, self._progress_key_schema_id, None, self._progress_value_schema_id)
        key = {
            'progress_kind': constants.SNAPSHOT_ROWS_PROGRESS_KIND,
            'topic_name': topic_name,
            'capture_instance_name': capture_instance_name
        }
        self.produce(self._progress_topic_name, key, self._progress_key_schema_id, None, self._progress_value_schema_id)
        logger.info('Deleted existing progress records for topic %s.', topic_name)

    def commit_progress(self, final: bool = False):
        if (datetime.datetime.now() - self._last_progress_commit_time) < constants.PROGRESS_COMMIT_INTERVAL \
                and not final:
            return

        start_time = time.perf_counter()
        full_flush_due = (datetime.datetime.now() - self._last_full_flush_time) > constants.FULL_FLUSH_MAX_INTERVAL
        flush_timeout = 30 if (final or full_flush_due) else 0.1
        still_in_queue_count = self._producer.flush(flush_timeout)  # triggers the _delivery_callback
        flushed_all = still_in_queue_count == 0
        if flushed_all:
            self._last_full_flush_time = datetime.datetime.now()

        for (kind, topic), progress_by_partition in self._progress_messages_awaiting_commit.items():
            commit_sequence, commit_partition = None, None
            if (not flushed_all) and len(progress_by_partition) < len(self._cluster_metadata.topics[topic].partitions):
                # not safe to commit anything if not all partitions have acked since there may still be some
                # less-progressed messages in flight for the non-acked partition:
                continue
            for partition, (produce_sequence, progress_msg) in progress_by_partition.items():
                if flushed_all:
                    if commit_sequence is None or produce_sequence > commit_sequence:
                        commit_sequence = produce_sequence
                        commit_partition = partition
                else:
                    if commit_sequence is None or produce_sequence < commit_sequence:
                        commit_sequence = produce_sequence
                        commit_partition = partition

            if commit_partition is not None:
                produce_sequence, progress_msg = progress_by_partition.pop(commit_partition)
                prior_sequence = self._commit_order_enforcer.get((kind, topic))
                if prior_sequence is not None and produce_sequence < prior_sequence:
                    raise Exception('Committing progress out of order. There is a bug. Fix it.')
                self._commit_order_enforcer[(kind, topic)] = produce_sequence
                key = {
                    'progress_kind': kind,
                    'topic_name': topic,
                    'capture_instance_name': self._capture_instance_name_resolver(topic)
                }
                self.produce(self._progress_topic_name, key, self._progress_key_schema_id, progress_msg,
                             self._progress_value_schema_id)

            if final:
                if self._producer.flush(30) > 0:
                    logger.error('Unable to complete final flush of Kafka producer queue.')

        if flushed_all:
            self._progress_messages_awaiting_commit = collections.defaultdict(dict)

        self._last_progress_commit_time = datetime.datetime.now()

        logger.debug('_commit_progress took %s ms (final=%s, flushed_all=%s)',
                     (time.perf_counter() - start_time) * 1000, final, flushed_all)

    def consume_all(self, topic_name: str) -> Generator[confluent_kafka.Message, None, None]:
        topic_metadata = self._cluster_metadata.topics.get(topic_name)

        if not topic_metadata:
            logger.warning(
                'consume_all: Requested topic %s does not appear to existing. Returning nothing.', topic_name)
            return

        self._consumer.assign([confluent_kafka.TopicPartition(topic_name, part_id, confluent_kafka.OFFSET_BEGINNING)
                               for part_id in topic_metadata.partitions.keys()])

        finished_parts = [False] * len(topic_metadata.partitions.keys())
        ctr = 0

        while True:
            msg = self._consumer.poll(self._kafka_timeout_seconds)

            if msg is None:
                time.sleep(1)
                continue
            if msg.error():
                if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    finished_parts[msg.partition()] = True
                    if all(finished_parts):
                        break
                    continue
                else:
                    raise confluent_kafka.KafkaException(msg.error())

            ctr += 1
            if ctr % 100000 == 0:
                logger.debug('consume_all has yielded %s messages so far from topic %s', ctr, topic_name)

            deserializer = self._avro_deserializers[msg.topic()]

            if msg.value() is not None:
                msg.set_value(deserializer.decode_message(msg.value(), is_key=False))
            if msg.key() is not None:
                msg.set_key(deserializer.decode_message(msg.key(), is_key=True))

            yield msg

    def create_topic(self, topic_name: str, partition_count: int, replication_factor: int = None,
                     extra_config: str = None):
        if not replication_factor:
            replication_factor = min(len(self._cluster_metadata.brokers), 3)

        topic_config = {'cleanup.policy': 'compact'}

        if extra_config:
            for extra in extra_config.split(';'):
                k, v = extra.split(':')
                topic_config[k] = v

        logger.debug('Kafka topic configuration for %s: \n%s', topic_name, json.dumps(topic_config, indent=4))
        topic = confluent_kafka.admin.NewTopic(topic_name, partition_count, replication_factor, config=topic_config)
        self._admin.create_topics([topic])[topic_name].result()

    def get_prior_progress_or_create_progress_topic(self, log_to_file_path: str = None) -> Dict[Tuple, Dict[str, Any]]:
        result = {}

        if self._progress_topic_name not in self._cluster_metadata.topics:
            logger.warning('No existing snapshot progress storage topic found; creating topic %s',
                           self._progress_topic_name)
            # log.segment.bytes set to 16 MB. Compaction will not run until the next log segment rolls so we set this
            # a bit low (the default is 1 GB) to prevent having to read too much from the topic on process start:
            self.create_topic(self._progress_topic_name, 1, extra_config='segment.bytes:16777216')
            time.sleep(5)
            self.refresh_cluster_metadata()
            return {}

        header = None
        if log_to_file_path is not None:
            all_file = open(os.path.join(log_to_file_path, 'all_progress_entries.csv'), 'w')
            all_csv_writer = csv.writer(all_file, quoting=csv.QUOTE_ALL)

        progress_msg_ctr = 0
        for progress_msg in self.consume_all(self._progress_topic_name):
            progress_msg_ctr += 1
            # Need to reform the key into a stably-sorted tuple of kv pairs so it can be used as a dictionary key:
            key = tuple(sorted(progress_msg.key().items(), key=lambda kv: kv[0]))

            if progress_msg.value() is None:
                if key in result:
                    del result[key]
                continue

            result[key] = dict(progress_msg.value())  # last read for a given key will win

            if log_to_file_path is not None:
                sorted_vals = tuple(sorted(progress_msg.value().items(), key=lambda kv: kv[0]))
                if not header:
                    header = ['timestamp', 'progress_offset'] + [key_k for key_k, _ in key] + \
                             [val_k for val_k, _ in sorted_vals]
                    all_csv_writer.writerow(header)
                timestamp = datetime.datetime.utcfromtimestamp(progress_msg.timestamp()[1] / 1000.0).isoformat()
                row = [timestamp, progress_msg.offset()] + [key_v for _, key_v in key] + \
                      [f'0x{val_v.hex()}' if isinstance(val_v, bytes) else val_v for _, val_v in sorted_vals]
                all_csv_writer.writerow(row)

        if log_to_file_path is not None:
            all_file.close()
            with open(os.path.join(log_to_file_path, 'latest_progress_entries.csv'), 'w') as latest_file:
                latest_csv_writer = csv.writer(latest_file, quoting=csv.QUOTE_ALL)
                latest_csv_writer.writerow(header[2:])
                for k, v in result.items():
                    sorted_vals = tuple(sorted(v.items(), key=lambda kv: kv[0]))
                    row = [key_v for _, key_v in k] + \
                          [f'0x{val_v.hex()}' if isinstance(val_v, bytes) else val_v for _, val_v in sorted_vals]
                    latest_csv_writer.writerow(row)

        logger.debug('Read %s prior progress messages from Kafka topic %s', progress_msg_ctr,
                     self._progress_topic_name)

        return result

    # Returns dict where key is topic name and value is ordered list of tuples of (low, high) watermarks per partition:
    def get_topic_watermarks(self, topic_names: List[str]) -> Dict[str, List[Tuple[int, int]]]:
        result = collections.defaultdict(list)

        for topic_name in topic_names:
            topic_metadata = self._cluster_metadata.topics.get(topic_name)

            if topic_metadata is None:
                logger.warning('Topic name %s was not found in Kafka. This process will create it if corresponding '
                               'CDC entries are found.', topic_name)
                continue

            partition_count = len(topic_metadata.partitions)

            for partition_ix in range(partition_count):
                watermarks = self._consumer.get_watermark_offsets(
                    confluent_kafka.TopicPartition(topic_name, partition_ix), timeout=self._kafka_timeout_seconds)
                if watermarks is None:
                    raise Exception(f'Timeout requesting watermark offsets from Kafka for topic {topic_name}, '
                                    f'partition {partition_ix}')
                result[topic_name].append(watermarks)

        return result

    def register_schemas(self, topic_name: str, key_schema: avro.schema.RecordSchema,
                         value_schema: avro.schema.RecordSchema) -> Tuple[int, int]:
        key_subject, value_subject = topic_name + '-key', topic_name + '-value'
        registered = False

        key_schema_id, current_key_schema, _ = self._schema_registry.get_latest_schema(key_subject)
        if current_key_schema is None or current_key_schema != key_schema:
            logger.info('Key schema for subject %s does not exist or is outdated; registering now.', key_subject)
            key_schema_id = self._schema_registry.register(key_subject, key_schema)
            self._schema_registry.update_compatibility(constants.KEY_SCHEMA_COMPATIBILITY_LEVEL, key_subject)
            registered = True

        value_schema_id, current_value_schema, _ = self._schema_registry.get_latest_schema(value_subject)
        if current_value_schema is None or current_value_schema != value_schema:
            logger.info('Value schema for subject %s does not exist or is outdated; registering now.', value_subject)
            value_schema_id = self._schema_registry.register(value_subject, value_schema)
            self._schema_registry.update_compatibility(constants.VALUE_SCHEMA_COMPATIBILITY_LEVEL, value_subject)
            registered = True

        if registered:
            # some older versions of the Confluent schema registry have a bug that leads to duplicate schema IDs in
            # some circumstances; delay a bit if we actually registered a new schema, to give the registry  a chance
            # to become consistent (see https://github.com/confluentinc/schema-registry/pull/1003 and linked issues
            # for context):
            time.sleep(3)

        self._avro_deserializers[topic_name] = confluent_kafka.avro.MessageSerializer(
                self._schema_registry, key_schema, value_schema)

        return key_schema_id, value_schema_id

    @staticmethod
    def _raise_kafka_error(err: confluent_kafka.KafkaError) -> None:
        if err.fatal():
            raise confluent_kafka.KafkaException(err)
        else:
            logger.warning("librdkafka raised a non-fatal error: code - %s, name - %s, msg - %s",
                           err.code(), err.name(), err.str())

    @staticmethod
    def _log_kafka_throttle_event(evt: confluent_kafka.ThrottleEvent) -> None:
        logger.warning('Kafka throttle event: %s', evt)

    def _delivery_callback(self, err: confluent_kafka.KafkaError, msg: confluent_kafka.Message,
                           orig_key: Dict[str, Any], orig_value: Dict[str, Any], produce_sequence: int) -> None:
        if err is not None:
            raise confluent_kafka.KafkaException(f'Delivery error on topic {msg.topic()}: {err}')

        self._successfully_delivered_messages_counter += 1
        if self._successfully_delivered_messages_counter % constants.KAFKA_DELIVERY_SUCCESS_LOG_EVERY_NTH_MSG == 0:
            logger.debug(f'{self._successfully_delivered_messages_counter} messages successfully produced '
                         f'to Kafka so far.')

        if msg.topic() == self._progress_topic_name or msg.value() is None:
            return

        progress_msg = {'last_ack_partition': msg.partition(), 'last_ack_offset': msg.offset()}

        if orig_value[constants.OPERATION_NAME] == constants.SNAPSHOT_OPERATION_NAME:
            kind = constants.SNAPSHOT_ROWS_PROGRESS_KIND
            progress_msg['last_ack_snapshot_key_field_values'] = orig_key
        else:
            kind = constants.CHANGE_ROWS_PROGRESS_KIND
            progress_msg['last_ack_change_table_lsn'] = orig_value[constants.LSN_NAME]
            progress_msg['last_ack_change_table_seqval'] = orig_value[constants.SEQVAL_NAME]
            progress_msg['last_ack_change_table_operation'] = \
                constants.CDC_OPERATION_NAME_TO_ID[orig_value[constants.OPERATION_NAME]]

        key = (kind, msg.topic())
        self._progress_messages_awaiting_commit[key][msg.partition()] = (produce_sequence, progress_msg)

