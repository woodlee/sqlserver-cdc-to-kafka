import collections
import datetime
import io
import json
import logging
import socket
import struct
import time
from typing import List, Dict, Tuple, Any, Callable, Union, Generator, Optional, Iterable

import confluent_kafka.admin
import confluent_kafka.avro
import fastavro

from . import constants

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


class KafkaClient(object):
    _instance = None
    TIMESTAMP_WARNING_LOGGED = False

    def __init__(self, metrics_accumulator: 'accumulator.AccumulatorAbstract', bootstrap_servers: str,
                 schema_registry_url: str, extra_kafka_consumer_config: Dict[str, Union[str, int]],
                 extra_kafka_producer_config: Dict[str, Union[str, int]], disable_writing: bool = False) -> None:
        if KafkaClient._instance is not None:
            raise Exception('KafkaClient class should be used as a singleton.')

        self._metrics_accumulator: 'accumulator.AccumulatorAbstract' = metrics_accumulator

        # Kafka consumer/producer librdkafka config defaults are here:
        consumer_config = {**{
            'bootstrap.servers': bootstrap_servers,
            'group.id': f'cdc_to_kafka_{socket.getfqdn()}',
            'enable.partition.eof': True,
            'enable.auto.commit': False
        }, **extra_kafka_consumer_config}
        producer_config = {**{
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': '50',
            'enable.idempotence': True,
            'statistics.interval.ms': 30 * 60 * 1000,
            'enable.gapless.guarantee': True,
            'retry.backoff.ms': 250,
            'compression.codec': 'snappy'
        }, **extra_kafka_producer_config}
        admin_config = {
            'bootstrap.servers': bootstrap_servers
        }

        logger.debug('Kafka consumer configuration: %s', json.dumps(consumer_config))
        logger.debug('Kafka producer configuration: %s', json.dumps(producer_config))
        logger.debug('Kafka admin client configuration: %s', json.dumps(admin_config))

        consumer_config['error_cb'] = KafkaClient._raise_kafka_error
        consumer_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        consumer_config['logger'] = logger
        producer_config['error_cb'] = KafkaClient._raise_kafka_error
        producer_config['stats_cb'] = KafkaClient._emit_producer_stats
        producer_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        producer_config['logger'] = logger
        admin_config['error_cb'] = KafkaClient._raise_kafka_error
        admin_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        admin_config['logger'] = logger

        self._schema_registry: confluent_kafka.avro.CachedSchemaRegistryClient = \
            confluent_kafka.avro.CachedSchemaRegistryClient(schema_registry_url)
        self._consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(consumer_config)
        self._producer: confluent_kafka.Producer = confluent_kafka.Producer(producer_config)
        self._admin: confluent_kafka.admin.AdminClient = confluent_kafka.admin.AdminClient(admin_config)
        self._avro_serializer: confluent_kafka.avro.MessageSerializer = \
            confluent_kafka.avro.MessageSerializer(self._schema_registry)
        self._avro_decoders: Dict[int, Callable] = {}
        self._delivery_callbacks: Dict[str, List[Callable]] = collections.defaultdict(list)
        self._delivery_callbacks_finalized: bool = False
        self._global_produce_sequence_nbr: int = 0
        self._cluster_metadata: Optional[confluent_kafka.admin.ClusterMetadata] = None
        self._last_full_flush_time: datetime.datetime = datetime.datetime.utcnow()
        self._disable_writing = disable_writing

        self._refresh_cluster_metadata()

        KafkaClient._instance = self

    def __enter__(self) -> 'KafkaClient':
        return self

    def __exit__(self, exc_type, value, traceback) -> None:
        logger.info("Cleaning up Kafka resources...")
        self._consumer.close()
        self.flush(final=True)
        logger.info("Done.")

    def register_delivery_callback(self, for_message_types: Iterable[str], callback: Callable[[Any], None]) -> None:
        for message_type in for_message_types:
            if message_type not in constants.ALL_KAFKA_MESSAGE_TYPES:
                raise Exception('Unrecognized message type: %s', message_type)
            self._delivery_callbacks[message_type].append(callback)

    # a return of None indicates the topic does not exist
    def get_topic_partition_count(self, topic_name: str) -> Optional[int]:
        if topic_name not in self._cluster_metadata.topics:
            return None
        return len(self._cluster_metadata.topics[topic_name].partitions)

    def produce(self, topic: str, key: Dict[str, Any], key_schema_id: int, value: Union[None, Dict[str, Any]],
                value_schema_id: int, message_type: str) -> None:
        if self._disable_writing:
            return

        start_time = time.perf_counter()

        key_ser = self._avro_serializer.encode_record_with_schema_id(key_schema_id, key, True)
        if value is None:  # a deletion tombstone probably
            value_ser = None
        else:
            value_ser = self._avro_serializer.encode_record_with_schema_id(value_schema_id, value, False)

        seq = self._global_produce_sequence_nbr + 1
        self._global_produce_sequence_nbr = seq

        while True:
            try:
                # the callback function receives the binary-serialized payload, so instead of specifying it directly
                # as the delivery callback we wrap it in a lambda that also captures and passes the original not-yet-
                # serialized key and value so that we don't have to re-deserialize it later:
                self._producer.produce(
                    topic=topic, value=value_ser, key=key_ser,
                    on_delivery=lambda err, msg: self._delivery_callback(message_type, err, msg, key, value, seq),
                    headers={'cdc_to_kafka_message_type': message_type}
                )
                break
            except BufferError:
                time.sleep(1)
                logger.debug('Sleeping due to Kafka producer buffer being full...')
                self.flush()  # clear some space before retrying
            except Exception:
                logger.error('The following exception occurred producing to topic %s', topic)
                raise

        elapsed = (time.perf_counter() - start_time)
        self._metrics_accumulator.register_kafka_produce(elapsed, value, message_type)

    def flush(self, final: bool = False) -> bool:
        if self._disable_writing:
            return True

        start_time = time.perf_counter()
        do_full_flush = final or ((datetime.datetime.utcnow() - self._last_full_flush_time) >
                                  constants.KAFKA_PRODUCER_FULL_FLUSH_INTERVAL)
        flush_timeout = constants.KAFKA_FULL_FLUSH_TIMEOUT_SECS if do_full_flush else 0.1
        still_in_queue_count = self._producer.flush(flush_timeout)  # this triggers delivery callbacks
        flushed_all = still_in_queue_count == 0

        if flushed_all:
            self._last_full_flush_time = datetime.datetime.utcnow()
        elif do_full_flush:
            logger.error('Could not complete full Kafka producer flush within %s second timeout. Messages not '
                         'flushed: %s', constants.KAFKA_FULL_FLUSH_TIMEOUT_SECS, still_in_queue_count)

        if final:  # Do it again to ensure flushing of anything that was produced by delivery callbacks:
            flushed_all = self._producer.flush(constants.KAFKA_FULL_FLUSH_TIMEOUT_SECS) == 0

        elapsed = time.perf_counter() - start_time
        self._metrics_accumulator.register_kafka_commit(elapsed)
        return flushed_all

    def consume_all(self, topic_name: str, approx_max_recs: Optional[int] = None) -> \
            Generator[confluent_kafka.Message, None, None]:
        part_count = self.get_topic_partition_count(topic_name)

        if part_count is None:
            logger.warning(
                'consume_all: Requested topic %s does not appear to exist. Returning nothing.', topic_name)
            return

        if approx_max_recs is None:
            self._consumer.assign([confluent_kafka.TopicPartition(topic_name, part_id, confluent_kafka.OFFSET_BEGINNING)
                                   for part_id in range(part_count)])
        else:
            rewind_per_part = int(approx_max_recs / part_count)
            watermarks = self.get_topic_watermarks([topic_name])[topic_name]
            offsets = [max(lo, hi - rewind_per_part) for lo, hi in watermarks]
            self._consumer.assign([confluent_kafka.TopicPartition(topic_name, part_id, offset)
                                   for part_id, offset in enumerate(offsets)])

        finished_parts = [False] * part_count
        ctr = 0

        while True:
            msg = self._consumer.poll(constants.KAFKA_REQUEST_TIMEOUT_SECS)

            if msg is None:
                time.sleep(1)
                continue
            if msg.error():
                # noinspection PyProtectedMember
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

            for msg_part, setter in ((msg.key(), msg.set_key), (msg.value(), msg.set_value)):
                if msg_part is not None:
                    payload = io.BytesIO(msg_part)
                    _, schema_id = struct.unpack('>bI', payload.read(5))
                    if schema_id not in self._avro_decoders:
                        self._set_decoder(schema_id)
                    decoder = self._avro_decoders[schema_id]
                    setter(decoder(payload))
                    payload.close()

            yield msg

    def _set_decoder(self, schema_id):
        schema = fastavro.parse_schema(self._schema_registry.get_by_id(schema_id).to_json())
        self._avro_decoders[schema_id] = lambda p: fastavro.schemaless_reader(p, schema)

    def create_topic(self, topic_name: str, partition_count: int, replication_factor: int = None,
                     extra_config: Dict[str, Union[str, int]] = None) -> None:
        if self._disable_writing:
            return

        if not replication_factor:
            replication_factor = min(len(self._cluster_metadata.brokers), 3)

        extra_config = extra_config or {}
        topic_config = {**{'cleanup.policy': 'compact'}, **extra_config}

        logger.debug('Kafka topic configuration for %s: %s', topic_name, json.dumps(topic_config))
        topic = confluent_kafka.admin.NewTopic(topic_name, partition_count, replication_factor, config=topic_config)
        self._admin.create_topics([topic])[topic_name].result()
        time.sleep(constants.KAFKA_CONFIG_RELOAD_DELAY_SECS)
        self._refresh_cluster_metadata()

    # Returns dict where key is topic name and value is ordered list of tuples of (low, high) watermarks per partition:
    def get_topic_watermarks(self, topic_names: List[str]) -> Dict[str, List[Tuple[int, int]]]:
        result = collections.defaultdict(list)

        for topic_name in topic_names:
            part_count = self.get_topic_partition_count(topic_name)

            if part_count is None:
                logger.warning('Topic name %s was not found in Kafka. This process will create it if corresponding '
                               'CDC entries are found.', topic_name)
                continue

            for part_id in range(part_count):
                watermarks = self._consumer.get_watermark_offsets(
                    confluent_kafka.TopicPartition(topic_name, part_id), timeout=constants.KAFKA_REQUEST_TIMEOUT_SECS)
                if watermarks is None:
                    raise Exception(f'Timeout requesting watermark offsets from Kafka for topic {topic_name}, '
                                    f'partition {part_id}')
                result[topic_name].append(watermarks)

        return result

    # returns (key schema ID, value schema ID)
    def register_schemas(self, topic_name: str, key_schema: Dict[str, Any], value_schema: Dict[str, Any],
                         key_schema_compatibility_level: str = constants.DEFAULT_KEY_SCHEMA_COMPATIBILITY_LEVEL,
                         value_schema_compatibility_level: str = constants.DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL) \
            -> Tuple[int, int]:
        key_schema = confluent_kafka.avro.loads(json.dumps(key_schema))
        value_schema = confluent_kafka.avro.loads(json.dumps(value_schema))

        key_subject, value_subject = topic_name + '-key', topic_name + '-value'
        registered = False

        key_schema_id, current_key_schema, _ = self._schema_registry.get_latest_schema(key_subject)
        if (current_key_schema is None or current_key_schema != key_schema) and not self._disable_writing:
            logger.info('Key schema for subject %s does not exist or is outdated; registering now.', key_subject)
            key_schema_id = self._schema_registry.register(key_subject, key_schema)
            if current_key_schema is None:
                time.sleep(constants.KAFKA_CONFIG_RELOAD_DELAY_SECS)
                self._schema_registry.update_compatibility(key_schema_compatibility_level, key_subject)
            registered = True

        value_schema_id, current_value_schema, _ = self._schema_registry.get_latest_schema(value_subject)
        if (current_value_schema is None or current_value_schema != value_schema) and not self._disable_writing:
            logger.info('Value schema for subject %s does not exist or is outdated; registering now.', value_subject)
            value_schema_id = self._schema_registry.register(value_subject, value_schema)
            if current_value_schema is None:
                time.sleep(constants.KAFKA_CONFIG_RELOAD_DELAY_SECS)
                self._schema_registry.update_compatibility(value_schema_compatibility_level, value_subject)
            registered = True

        if registered:
            # some older versions of the Confluent schema registry have a bug that leads to duplicate schema IDs in
            # some circumstances; delay a bit if we actually registered a new schema, to give the registry a chance
            # to become consistent (see https://github.com/confluentinc/schema-registry/pull/1003 and linked issues
            # for context):
            time.sleep(constants.KAFKA_CONFIG_RELOAD_DELAY_SECS)

        return key_schema_id, value_schema_id

    def _refresh_cluster_metadata(self) -> None:
        self._cluster_metadata = self._admin.list_topics(timeout=constants.KAFKA_REQUEST_TIMEOUT_SECS)
        if self._cluster_metadata is None:
            raise Exception(f'Cluster metadata request to Kafka timed out')

    def _delivery_callback(self, message_type: str, err: confluent_kafka.KafkaError, message: confluent_kafka.Message,
                           original_key: Dict[str, Any], original_value: Dict[str, Any], produce_sequence: int) -> None:
        if err is not None:
            raise confluent_kafka.KafkaException(f'Delivery error on topic {message.topic()}: {err}')

        timestamp_type, timestamp = message.timestamp()
        if timestamp_type != confluent_kafka.TIMESTAMP_CREATE_TIME:
            if not KafkaClient.TIMESTAMP_WARNING_LOGGED:
                logger.warning('Kafka message producer timestamps not available; falling back to delivery '
                               'callback times for measuring E2E latencies.')
                KafkaClient.TIMESTAMP_WARNING_LOGGED = True
            produce_datetime = datetime.datetime.utcnow()
        else:
            produce_datetime = datetime.datetime.utcfromtimestamp(timestamp / 1000.0)

        for cb in self._delivery_callbacks[message_type]:
            cb(message_type=message_type,
               message=message,
               original_key=original_key,
               original_value=original_value,
               produce_sequence=produce_sequence,
               produce_datetime=produce_datetime)

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

    @staticmethod
    def _emit_producer_stats(stats_json: str) -> None:
        logger.info('Kafka producer statistics: %s', stats_json)
