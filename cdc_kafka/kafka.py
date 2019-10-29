import collections
import datetime
import json
import logging
import socket
import time
from typing import List, Dict, Tuple, Any, Callable, Union

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
                 progress_message_extractor: Callable[[str, Dict[str, Any]],
                                                      Tuple[Dict[str, Any], avro.schema.RecordSchema,
                                                            Dict[str, Any], avro.schema.RecordSchema]],
                 extra_kafka_consumer_config: str,
                 extra_kafka_producer_config: str):

        if KafkaClient.__instance is not None:
            raise Exception('KafkaClient class should be used as a singleton.')

        self._kafka_timeout_seconds: int = kafka_timeout_seconds
        self._progress_topic_name: str = progress_topic_name
        self._progress_message_extractor: Callable = progress_message_extractor

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
        self._serializer = confluent_kafka.avro.MessageSerializer(self._schema_registry)
        self._consumer = confluent_kafka.avro.AvroConsumer(consumer_config, schema_registry=self._schema_registry)
        self._producer = confluent_kafka.Producer(producer_config)
        self._admin = confluent_kafka.admin.AdminClient(admin_config)

        self._refresh_cluster_metadata()

        self._last_progress_commit_time: datetime.datetime = datetime.datetime.now()
        self._successfully_delivered_messages_counter: int = 0
        self._progress_messages_awaiting_commit: Dict[Tuple, Tuple] = {}

        k_id, v_id = self.register_schemas(
            self._progress_topic_name, constants.PROGRESS_MESSAGE_AVRO_KEY_SCHEMA,
            constants.PROGRESS_MESSAGE_AVRO_VALUE_SCHEMA)
        self._progress_key_schema_id: int = k_id
        self._progress_value_schema_id: int = v_id

        KafkaClient.__instance = self

    def _refresh_cluster_metadata(self):
        self._cluster_metadata = self._admin.list_topics(timeout=self._kafka_timeout_seconds)
        if self._cluster_metadata is None:
            raise Exception(f'Cluster metadata request to Kafka timed out')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        logger.info("Committing progress and cleaning up Kafka resources...")
        self._consumer.close()
        self._commit_progress(True)
        logger.info("Done.")

    def produce(self, topic: str, key: Dict[str, Any], key_schema_id: int, value: Union[None, Dict[str, Any]],
                value_schema_id: int) -> None:
        while True:
            try:
                key_ser = self._serializer.encode_record_with_schema_id(key_schema_id, key, True)
                value_ser = self._serializer.encode_record_with_schema_id(value_schema_id, value, False)
                self._producer.produce(topic=topic, value=value_ser, key=key_ser,
                                       callback=lambda err, msg: self._delivery_callback(err, msg, value))
                break
            except BufferError:
                time.sleep(1)
                logger.debug('Sleeping due to Kafka producer buffer being full...')
            except Exception:
                logger.error('The following exception occurred producing to topic %', topic)
                raise

        if topic != self._progress_topic_name and (datetime.datetime.now() - self._last_progress_commit_time) > \
                constants.PROGRESS_COMMIT_INTERVAL:
            self._commit_progress()
            self._last_progress_commit_time = datetime.datetime.now()

    def consume_all(self, topic_name: str):
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
            if msg.error() and msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                finished_parts[msg.partition()] = True
                if all(finished_parts):
                    break
            if msg.error():
                raise confluent_kafka.KafkaException(msg.error())

            ctr += 1
            if ctr % 100000 == 0:
                logger.debug('consume_all has yielded %s messages so far from topic %s', ctr, topic_name)

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
        self._refresh_cluster_metadata()

    def get_prior_progress_or_create_progress_topic(self) -> Dict[Tuple, Dict[str, Any]]:
        result = {}

        if self._progress_topic_name not in self._cluster_metadata.topics:
            logger.warning('No existing snapshot progress storage topic found; creating topic %s',
                           self._progress_topic_name)
            self.create_topic(self._progress_topic_name, 1)
            return {}

        progress_msg_ctr = 0
        for progress_msg in self.consume_all(self._progress_topic_name):
            progress_msg_ctr += 1
            # Need to reform the key into a stably-sorted tuple of kv pairs so it can be used as a dictionary key:
            key = tuple(sorted(progress_msg.key().items(), key=lambda kv: kv[0]))
            result[key] = progress_msg.value()  # last read for a given key will win

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
                           orig_message_value: Dict[str, Any]) -> None:
        if err is not None:
            raise confluent_kafka.KafkaException(f'Delivery error on topic {msg.topic()}: {err}')

        self._successfully_delivered_messages_counter += 1
        if self._successfully_delivered_messages_counter % constants.KAFKA_DELIVERY_SUCCESS_LOG_EVERY_NTH_MSG == 0:
            logger.debug(f'{self._successfully_delivered_messages_counter} messages successfully produced '
                         f'to Kafka so far.')

        if msg.topic() == self._progress_topic_name or msg.value() is None:
            return

        key, key_schema, value, value_schema = self._progress_message_extractor(msg.topic(), orig_message_value)
        self._progress_messages_awaiting_commit[(tuple(key.items()), key_schema)] = (value, value_schema)

    def _commit_progress(self, final=False):
        start_time = time.perf_counter()
        self._producer.flush(30 if final else 0.1)  # triggers the _delivery_callback

        for (key_kvs, key_schema), (value, value_schema) in self._progress_messages_awaiting_commit.items():
            self.produce(self._progress_topic_name, dict(key_kvs), self._progress_key_schema_id, value,
                         self._progress_value_schema_id)

        if final:
            self._producer.flush(30)

        self._progress_messages_awaiting_commit = {}
        logger.debug('_commit_progress took %s ms', (time.perf_counter() - start_time) * 1000)
