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
            'linger.ms': '100',
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
        self._consumer = confluent_kafka.avro.AvroConsumer(consumer_config, schema_registry=self._schema_registry)
        self._producer = confluent_kafka.avro.AvroProducer(producer_config, schema_registry=self._schema_registry)
        self._admin = confluent_kafka.admin.AdminClient(admin_config)

        self._refresh_cluster_metadata()

        self._last_progress_commit_time: datetime.datetime = datetime.datetime.now()
        self._successfully_delivered_messages_counter: int = 0
        self._progress_messages_awaiting_commit: Dict[Tuple, Tuple] = {}

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
        self._commit_progress()
        logger.info("Done.")

    def produce(self, topic: str, key: Dict[str, Any], key_schema: avro.schema.RecordSchema,
                value: Union[None, Dict[str, Any]], value_schema: avro.schema.RecordSchema) -> None:
        while True:
            try:
                self._producer.produce(topic=topic, key=key, key_schema=key_schema, value=value,
                                       value_schema=value_schema,
                                       callback=lambda err, msg: self._delivery_callback(err, msg, value))
                break
            except BufferError:
                time.sleep(1)

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

    def get_prior_progress_or_create_progress_topic(self) -> Dict[Tuple, Dict[str, Any]]:
        result = {}

        if self._progress_topic_name not in self._cluster_metadata.topics:
            logger.warning('No existing snapshot progress storage topic found; creating topic %s',
                           self._progress_topic_name)
            replication_factor = min(len(self._cluster_metadata.brokers), 3)
            progress_topic = confluent_kafka.admin.NewTopic(self._progress_topic_name, 1, replication_factor,
                                                            config={'cleanup.policy': 'compact'})
            self._admin.create_topics([progress_topic])[self._progress_topic_name].result()
            self._refresh_cluster_metadata()
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
                    raise Exception(f'Timeout request watermark offsets from Kafka for topic {topic_name}, '
                                    f'partition {partition_ix}')
                result[topic_name].append(watermarks)

        return result

    def register_schemas(self, topic_name, key_schema, value_schema):
        key_subject, value_subject = topic_name + '-key', topic_name + '-value'
        self._schema_registry.register(key_subject, key_schema)
        self._schema_registry.register(value_subject, value_schema)
        self._schema_registry.update_compatibility(constants.KEY_SCHEMA_COMPATIBILITY_LEVEL, key_subject)
        self._schema_registry.update_compatibility(constants.VALUE_SCHEMA_COMPATIBILITY_LEVEL, value_subject)

    @staticmethod
    def _raise_kafka_error(err: confluent_kafka.KafkaError) -> None:
        raise confluent_kafka.KafkaException(err)

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

    def _commit_progress(self):
        self._producer.flush(self._kafka_timeout_seconds)  # triggers the _delivery_callback

        for (key_kvs, key_schema), (value, value_schema) in self._progress_messages_awaiting_commit.items():
            self.produce(self._progress_topic_name, dict(key_kvs), key_schema, value, value_schema)

        self._producer.flush(self._kafka_timeout_seconds)
        self._progress_messages_awaiting_commit = {}
