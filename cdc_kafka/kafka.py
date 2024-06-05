import collections
import inspect
import io
import json
import logging
import socket
import struct
import time
from types import TracebackType
from typing import List, Dict, Tuple, Any, Callable, Generator, Optional, Iterable, Set, Type

from avro.schema import Schema
import confluent_kafka.admin
import confluent_kafka.avro
import fastavro

from . import constants, kafka_oauth

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


class KafkaClient(object):
    _instance = None

    def __init__(self, metrics_accumulator: 'accumulator.AccumulatorAbstract', bootstrap_servers: str,
                 schema_registry_url: str, extra_kafka_consumer_config: Dict[str, str | int],
                 extra_kafka_producer_config: Dict[str, str | int], disable_writing: bool = False,
                 transactional_id: Optional[str] = None) -> None:
        if KafkaClient._instance is not None:
            raise Exception('KafkaClient class should be used as a singleton.')

        self._metrics_accumulator: 'accumulator.AccumulatorAbstract' = metrics_accumulator

        # Kafka consumer/producer librdkafka config defaults are here:
        self.consumer_config: Dict[str, Any] = {**{
            'bootstrap.servers': bootstrap_servers,
            'group.id': f'cdc_to_kafka_{socket.getfqdn()}',
            'enable.partition.eof': True,
            'enable.auto.commit': False
        }, **extra_kafka_consumer_config}
        producer_config: Dict[str, Any] = {**{
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': '200',
            'enable.idempotence': True,
            'statistics.interval.ms': 30 * 60 * 1000,
            'enable.gapless.guarantee': True,
            'retry.backoff.ms': 250,
            'compression.codec': 'snappy'
        }, **extra_kafka_producer_config}
        admin_config: Dict[str, Any] = {
            'bootstrap.servers': bootstrap_servers
        }

        oauth_provider = kafka_oauth.get_kafka_oauth_provider()

        self._use_oauth: bool = False
        if oauth_provider is not None:
            self._use_oauth = True
            logger.debug('Using Kafka OAuth provider class %s', type(oauth_provider).__name__)
            for config_dict in (self.consumer_config, producer_config, admin_config):
                if not config_dict.get('security.protocol'):
                    config_dict['security.protocol'] = 'SASL_SSL'
                if not config_dict.get('sasl.mechanisms'):
                    config_dict['sasl.mechanisms'] = 'OAUTHBEARER'
                if not config_dict.get('client.id'):
                    config_dict['client.id'] = socket.gethostname()

        logger.debug('Kafka consumer configuration: %s', json.dumps(self.consumer_config))
        logger.debug('Kafka producer configuration: %s', json.dumps(producer_config))
        logger.debug('Kafka admin client configuration: %s', json.dumps(admin_config))

        self.consumer_config['error_cb'] = KafkaClient._raise_kafka_error
        self.consumer_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        self.consumer_config['logger'] = logger
        producer_config['error_cb'] = KafkaClient._raise_kafka_error
        producer_config['stats_cb'] = KafkaClient._emit_producer_stats
        producer_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        producer_config['logger'] = logger
        admin_config['error_cb'] = KafkaClient._raise_kafka_error
        admin_config['throttle_cb'] = KafkaClient._log_kafka_throttle_event
        admin_config['logger'] = logger

        if oauth_provider is not None:
            self.consumer_config['oauth_cb'] = oauth_provider.consumer_oauth_cb
            producer_config['oauth_cb'] = oauth_provider.producer_oauth_cb
            admin_config['oauth_cb'] = oauth_provider.admin_oauth_cb

        self._use_transactions: bool = False
        if transactional_id is not None:
            producer_config['transactional.id'] = transactional_id
            self._use_transactions = True

        self._producer: confluent_kafka.Producer = confluent_kafka.Producer(producer_config)
        self._schema_registry: confluent_kafka.avro.CachedSchemaRegistryClient = \
            confluent_kafka.avro.CachedSchemaRegistryClient(schema_registry_url)
        self._admin: confluent_kafka.admin.AdminClient = confluent_kafka.admin.AdminClient(admin_config)
        self._avro_serializer: confluent_kafka.avro.MessageSerializer = \
            confluent_kafka.avro.MessageSerializer(self._schema_registry)
        self._avro_decoders: Dict[int, Callable[[io.BytesIO], Dict[str, Any]]] = dict()
        self._schema_ids_to_names: Dict[int, str] = dict()
        self._delivery_callbacks: Dict[str, List[Callable[
            [str, confluent_kafka.Message, Optional[Dict[str, Any]], Optional[Dict[str, Any]]], None
        ]]] = collections.defaultdict(list)
        self._disable_writing = disable_writing
        self._creation_warned_topic_names: Set[str] = set()

        if self._use_oauth:  # trigger initial oauth_cb calls
            self._producer.poll(constants.KAFKA_OAUTH_CB_POLL_TIMEOUT)

        if self._use_transactions and not self._disable_writing:
            self._producer.init_transactions(constants.KAFKA_REQUEST_TIMEOUT_SECS)

        self._cluster_metadata: confluent_kafka.admin.ClusterMetadata = self._get_cluster_metadata()

        KafkaClient._instance = self

    @staticmethod
    def get_instance() -> 'KafkaClient':
        if not KafkaClient._instance:
            raise Exception('KafkaClient has not yet been instantiated.')
        return KafkaClient._instance

    def __enter__(self) -> 'KafkaClient':
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        logger.info("Cleaning up Kafka resources...")
        if not self._disable_writing:
            self._producer.flush(constants.KAFKA_FULL_FLUSH_TIMEOUT_SECS)
        del self._admin
        del self._producer
        time.sleep(1)  # gives librdkafka threads more of a chance to exit properly before admin/producer are GC'd
        logger.info("Done.")

    def register_delivery_callback(self, for_message_types: Iterable[str],
                                   callback: Callable[[str, confluent_kafka.Message, Optional[Dict[str, Any]],
                                                       Optional[Dict[str, Any]]], None]) -> None:
        for message_type in for_message_types:
            if message_type not in constants.ALL_KAFKA_MESSAGE_TYPES:
                raise Exception('Unrecognized message type: %s', message_type)
            self._delivery_callbacks[message_type].append(callback)

    # a return of None indicates the topic does not exist
    def get_topic_partition_count(self, topic_name: str) -> int:
        if topic_name not in self._cluster_metadata.topics:
            return 0
        return len(self._cluster_metadata.topics[topic_name].partitions)

    def begin_transaction(self) -> None:
        if not self._use_transactions:
            raise Exception('This instance of KafkaClient was not configured to use transactions.')
        if self._disable_writing:
            return
        if logger.isEnabledFor(logging.DEBUG):
            current_frame = inspect.currentframe()
            if current_frame and current_frame.f_back:
                previous_frame = inspect.getframeinfo(current_frame.f_back)
                logger.debug('Kafka transaction begin from %s', f'{previous_frame[0]}, line {previous_frame[1]}')
        self._producer.begin_transaction()

    def commit_transaction(self) -> None:
        if not self._use_transactions:
            raise Exception('This instance of KafkaClient was not configured to use transactions.')
        if self._disable_writing:
            return
        if logger.isEnabledFor(logging.DEBUG):
            current_frame = inspect.currentframe()
            if current_frame and current_frame.f_back:
                previous_frame = inspect.getframeinfo(current_frame.f_back)
                logger.debug('Kafka transaction commit from %s', f'{previous_frame[0]}, line {previous_frame[1]}')
        self._producer.commit_transaction()

    def produce(self, topic: str, key: Optional[Dict[str, Any]], key_schema_id: int, value: Optional[Dict[str, Any]],
                value_schema_id: int, message_type: str, copy_to_unified_topics: Optional[List[str]] = None,
                extra_headers: Optional[Dict[str, str | bytes]] = None) -> None:
        if self._disable_writing:
            return

        start_time = time.perf_counter()
        if key is None:
            key_ser = None
        else:
            key_ser = self._avro_serializer.encode_record_with_schema_id(key_schema_id, key, True)

        if value is None:  # a deletion tombstone probably
            value_ser = None
        else:
            value_ser = self._avro_serializer.encode_record_with_schema_id(value_schema_id, value, False)

        while True:
            try:
                # the callback function receives the binary-serialized payload, so instead of specifying it
                # directly as the delivery callback we wrap it in a lambda that also captures and passes the
                # original not-yet-serialized key and value so that we don't have to re-deserialize it later:
                self._producer.produce(
                    topic=topic, value=value_ser, key=key_ser,
                    on_delivery=lambda err, msg: self._delivery_callback(message_type, err, msg, key, value),
                    headers={'cdc_to_kafka_message_type': message_type, **(extra_headers or {})}
                )
                break
            except BufferError:
                logger.warning('Sleeping due to Kafka producer buffer being full...')
                self._producer.flush(3)  # clear some space before retrying
            except Exception:
                logger.error('The following exception occurred producing to topic %s', topic)
                raise

        elapsed = (time.perf_counter() - start_time)
        self._metrics_accumulator.register_kafka_produce(elapsed, value, message_type)

        if copy_to_unified_topics:
            for unified_topic in copy_to_unified_topics:
                start_time = time.perf_counter()

                while True:
                    try:
                        self._producer.produce(
                            topic=unified_topic, value=value_ser, key=key_ser,
                            on_delivery=lambda err, msg: self._delivery_callback(
                                constants.UNIFIED_TOPIC_CHANGE_MESSAGE, err, msg, key, value),
                            headers={'cdc_to_kafka_message_type': constants.UNIFIED_TOPIC_CHANGE_MESSAGE,
                                     'cdc_to_kafka_original_topic': topic, **(extra_headers or {})}
                        )
                        break
                    except BufferError:
                        logger.warning('Sleeping due to Kafka producer buffer being full...')
                        self._producer.flush(3)  # clear some space before retrying
                    except Exception:
                        logger.error('The following exception occurred producing to topic %s', unified_topic)
                        raise

                elapsed = (time.perf_counter() - start_time)
                self._metrics_accumulator.register_kafka_produce(elapsed, value, constants.UNIFIED_TOPIC_CHANGE_MESSAGE)

    def consume_all(self, topic_name: str) -> Generator[confluent_kafka.Message, None, None]:
        part_count = self.get_topic_partition_count(topic_name)

        if part_count is None:
            logger.warning(
                'consume_all: Requested topic %s does not appear to exist. Returning nothing.', topic_name)
            return

        watermarks = self.get_topic_watermarks([topic_name])[topic_name]  # will be list of (low, hi) mark tuples
        last_offset = sum(x[1] for x in watermarks)
        if not last_offset:
            logger.warning(
                'consume_all: Requested topic %s contains no messages at present. Returning nothing.', topic_name)
            return
        logger.debug('Progress topic %s ends at offset %s', topic_name, last_offset)

        consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(self.consumer_config)
        if self._use_oauth:
            consumer.poll(constants.KAFKA_OAUTH_CB_POLL_TIMEOUT)  # Trigger initial oauth_cb call

        consumer.assign([confluent_kafka.TopicPartition(topic_name, part_id, confluent_kafka.OFFSET_BEGINNING)
                         for part_id in range(part_count)])

        finished_parts = [False] * part_count
        ctr = 0

        while True:
            msg = consumer.poll(constants.KAFKA_REQUEST_TIMEOUT_SECS)

            if msg is None:
                time.sleep(0.2)
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

            self._set_decoded_msg(msg)
            yield msg

        consumer.close()

    def consume_bounded(self, topic_name: str, approx_max_recs: int,
                        boundary_watermarks: List[Tuple[int, int]]) -> Generator[confluent_kafka.Message, None, None]:
        part_count = self.get_topic_partition_count(topic_name)

        if part_count is None:
            logger.warning(
                'consume_bounded: Requested topic %s does not appear to exist. Returning nothing.', topic_name)
            return

        if part_count != len(boundary_watermarks):
            raise ValueError('consume_bounded: The number of captured watermarks does not match the number of '
                             'partitions for topic %s', topic_name)

        rewind_per_part = int(approx_max_recs / part_count)
        start_offsets = [max(lo, hi - rewind_per_part) for lo, hi in boundary_watermarks]

        consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(self.consumer_config)
        if self._use_oauth:
            consumer.poll(constants.KAFKA_OAUTH_CB_POLL_TIMEOUT)  # Trigger initial oauth_cb call

        consumer.assign([confluent_kafka.TopicPartition(topic_name, part_id, offset)
                         for part_id, offset in enumerate(start_offsets)])

        finished_parts = [False] * part_count
        ctr = 0

        while True:
            msg = consumer.poll(constants.KAFKA_REQUEST_TIMEOUT_SECS)

            if msg is None:
                time.sleep(0.2)
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
            if msg.offset() > boundary_watermarks[msg.partition()][1]:
                finished_parts[msg.partition()] = True
                if all(finished_parts):
                    break
                continue

            ctr += 1
            if ctr % 100000 == 0:
                logger.debug('consume_bounded has yielded %s messages so far from topic %s', ctr, topic_name)

            self._set_decoded_msg(msg)
            yield msg

        consumer.close()

    def _set_decoded_msg(self, msg: confluent_kafka.Message) -> None:
        # noinspection PyArgumentList
        for msg_part, setter in ((msg.key(), msg.set_key), (msg.value(), msg.set_value)):
            if msg_part is not None:
                payload = io.BytesIO(msg_part)
                _, schema_id = struct.unpack('>bI', payload.read(5))
                if schema_id not in self._avro_decoders:
                    self._set_decoder(schema_id)
                decoder = self._avro_decoders[schema_id]
                to_set = decoder(payload)
                to_set['__avro_schema_name'] = self._schema_ids_to_names[schema_id]
                setter(to_set)
                payload.close()

    def _set_decoder(self, schema_id: int) -> None:
        reg_schema = self._schema_registry.get_by_id(schema_id)
        schema = fastavro.parse_schema(reg_schema.to_json())
        self._schema_ids_to_names[schema_id] = reg_schema.name
        self._avro_decoders[schema_id] = lambda p: fastavro.schemaless_reader(p, schema)

    def create_topic(self, topic_name: str, partition_count: int, replication_factor: Optional[int] = None,
                     extra_config: Optional[Dict[str, str | int]] = None) -> None:
        if self._disable_writing:
            return

        if not replication_factor:
            replication_factor = min(len(self._cluster_metadata.brokers), 3)

        extra_config = extra_config or {}
        topic_config = {**{'cleanup.policy': 'compact'}, **extra_config}

        logger.info('Creating Kafka topic "%s" with %s partitions, replication factor %s, and config: %s', topic_name,
                    partition_count, replication_factor, json.dumps(topic_config))
        topic = confluent_kafka.admin.NewTopic(topic_name, partition_count, replication_factor, config=topic_config)
        self._admin.create_topics([topic])[topic_name].result()
        time.sleep(constants.KAFKA_CONFIG_RELOAD_DELAY_SECS)
        self._refresh_cluster_metadata()

    # Returns dict where key is topic name and value is ordered list of tuples of (low, high) watermarks per partition:
    def get_topic_watermarks(self, topic_names: List[str]) -> Dict[str, List[Tuple[int, int]]]:
        result = collections.defaultdict(list)

        consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(self.consumer_config)
        if self._use_oauth:
            consumer.poll(constants.KAFKA_OAUTH_CB_POLL_TIMEOUT)  # In case oauth token refresh is needed

        for topic_name in topic_names:
            part_count = self.get_topic_partition_count(topic_name)

            if part_count is None:
                if topic_name not in self._creation_warned_topic_names:
                    logger.warning('Topic name %s was not found in Kafka. This process will create it if corresponding '
                                   'CDC entries are found.', topic_name)
                    self._creation_warned_topic_names.add(topic_name)
                continue

            for part_id in range(part_count):
                watermarks = consumer.get_watermark_offsets(
                    confluent_kafka.TopicPartition(topic_name, part_id), timeout=constants.KAFKA_REQUEST_TIMEOUT_SECS)
                if watermarks is None:
                    raise Exception(f'Timeout requesting watermark offsets from Kafka for topic {topic_name}, '
                                    f'partition {part_id}')
                result[topic_name].append(watermarks)

        consumer.close()
        return result

    def get_topic_config(self, topic_name: str) -> Any:
        resource = confluent_kafka.admin.ConfigResource(
            restype=confluent_kafka.admin.ConfigResource.Type.TOPIC, name=topic_name)
        result = self._admin.describe_configs([resource])
        return result[resource].result()

    # returns (key schema ID, value schema ID)
    def register_schemas(self, topic_name: str, key_schema: Optional[Schema], value_schema: Schema,
                         key_schema_compatibility_level: str = constants.DEFAULT_KEY_SCHEMA_COMPATIBILITY_LEVEL,
                         value_schema_compatibility_level: str = constants.DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL) \
            -> Tuple[int, int]:
        # TODO: it turns out that if you try to re-register a schema that was previously registered but later superseded
        # (e.g. in the case of adding and then later deleting a column), the schema registry will accept that and return
        # you the previously-registered schema ID without updating the `latest` version associated with the registry
        # subject, or verifying that the change is Avro-compatible. It seems like the way to handle this, per
        # https://github.com/confluentinc/schema-registry/issues/1685, would be to detect the condition and delete the
        # subject-version-number of that schema before re-registering it. Since subject-version deletion is not
        # available in the `CachedSchemaRegistryClient` we use here--and since this is a rare case--I'm explicitly
        # choosing to punt on it for the moment. The Confluent lib does now have a newer `SchemaRegistryClient` class
        # which supports subject-version deletion, but changing this code to use it appears to be a non-trivial task.

        key_subject, value_subject = topic_name + '-key', topic_name + '-value'
        registered = False

        if key_schema:
            key_schema_id, current_key_schema, _ = self._schema_registry.get_latest_schema(key_subject)
            if (current_key_schema is None or current_key_schema != key_schema) and not self._disable_writing:
                logger.info('Key schema for subject %s does not exist or is outdated; registering now.', key_subject)
                key_schema_id = self._schema_registry.register(key_subject, key_schema)
                logger.debug('Schema registered for subject %s: %s', key_subject, key_schema)
                if current_key_schema is None:
                    time.sleep(constants.KAFKA_CONFIG_RELOAD_DELAY_SECS)
                    self._schema_registry.update_compatibility(key_schema_compatibility_level, key_subject)
                registered = True
        else:
            key_schema_id = 0

        value_schema_id, current_value_schema, _ = self._schema_registry.get_latest_schema(value_subject)
        if (current_value_schema is None or current_value_schema != value_schema) and not self._disable_writing:
            logger.info('Value schema for subject %s does not exist or is outdated; registering now.', value_subject)
            value_schema_id = self._schema_registry.register(value_subject, value_schema)
            logger.debug('Schema registered for subject %s: %s', value_subject, value_schema)
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
        self._cluster_metadata = self._get_cluster_metadata()

    def _get_cluster_metadata(self) -> confluent_kafka.admin.ClusterMetadata:
        if self._use_oauth:
            self._admin.poll(constants.KAFKA_OAUTH_CB_POLL_TIMEOUT)  # In case oauth token refresh is needed
        metadata = self._admin.list_topics(timeout=constants.KAFKA_REQUEST_TIMEOUT_SECS)
        if metadata is None:
            raise Exception(f'Cluster metadata request to Kafka timed out')
        return metadata

    def _delivery_callback(self, message_type: str, err: confluent_kafka.KafkaError, message: confluent_kafka.Message,
                           original_key: Optional[Dict[str, Any]], original_value: Optional[Dict[str, Any]]) -> None:
        if err is not None:
            raise confluent_kafka.KafkaException(f'Delivery error on topic {message.topic()}: {err}')

        for cb in self._delivery_callbacks[message_type]:
            cb(message_type, message, original_key, original_value)

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
