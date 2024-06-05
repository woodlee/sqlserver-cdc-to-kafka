import collections
import datetime
import inspect
import json
import logging
import socket
import time
from types import TracebackType
from typing import List, Dict, Tuple, Any, Generator, Optional, Set, Type

import confluent_kafka.admin

from . import constants, kafka_oauth

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


class KafkaClient(object):
    _instance = None

    def __init__(self, metrics_accumulator: 'accumulator.AccumulatorAbstract', bootstrap_servers: str,
                 extra_kafka_consumer_config: Dict[str, str | int], extra_kafka_producer_config: Dict[str, str | int],
                 disable_writing: bool = False, transactional_id: Optional[str] = None) -> None:
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
        self._admin: confluent_kafka.admin.AdminClient = confluent_kafka.admin.AdminClient(admin_config)
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

    # a return of None indicates the topic does not exist
    def get_topic_partition_count(self, topic_name: str) -> int:
        if self._cluster_metadata.topics is None:
            raise Exception('Unexpected state: no topic metadata')
        if topic_name not in self._cluster_metadata.topics:
            return 0
        return len(self._cluster_metadata.topics[topic_name].partitions or [])

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

    def produce(self, topic: str, key: Optional[bytes], value: Optional[bytes], message_type: str,
                copy_to_unified_topics: Optional[List[str]] = None, event_datetime: Optional[datetime.datetime] = None,
                change_lsn: Optional[bytes] = None, operation_id: Optional[int] = None,
                extra_headers: Optional[Dict[str, str | bytes]] = None) -> None:
        if self._disable_writing:
            return

        start_time = time.perf_counter()

        if event_datetime:
            delivery_cb = lambda _, msg: self._metrics_accumulator.kafka_delivery_callback(msg, event_datetime)
        else:
            delivery_cb = None

        while True:
            try:
                self._producer.produce(
                    topic=topic, value=value, key=key, on_delivery=delivery_cb,
                    headers={'cdc_to_kafka_message_type': message_type, **(extra_headers or {})}
                )
                break
            except BufferError:
                logger.warning('Sleeping due to Kafka producer buffer being full...')
                self._producer.flush(3)  # clear some space before retrying
            except Exception:
                logger.error('The following exception occurred producing to topic %s', topic)
                raise

        elapsed = time.perf_counter() - start_time
        self._metrics_accumulator.register_kafka_produce(elapsed, message_type, event_datetime,
                                                         change_lsn, operation_id)

        if copy_to_unified_topics:
            for unified_topic in copy_to_unified_topics:
                start_time = time.perf_counter()

                while True:
                    try:
                        self._producer.produce(
                            topic=unified_topic, value=value, key=key, on_delivery=delivery_cb,
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

                elapsed = time.perf_counter() - start_time
                self._metrics_accumulator.register_kafka_produce(elapsed, constants.UNIFIED_TOPIC_CHANGE_MESSAGE,
                                                                 event_datetime, change_lsn, operation_id)

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
                if (msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF  # type: ignore[union-attr]
                        and msg.partition() is not None):
                    finished_parts[msg.partition()] = True  # type: ignore[index]
                    if all(finished_parts):
                        break
                    continue
                else:
                    raise confluent_kafka.KafkaException(msg.error())

            ctr += 1
            if ctr % 100000 == 0:
                logger.debug('consume_all has yielded %s messages so far from topic %s', ctr, topic_name)

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
                if (msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF  # type: ignore[union-attr]
                        and msg.partition() is not None):
                    finished_parts[msg.partition()] = True  # type: ignore[index]
                    if all(finished_parts):
                        break
                    continue
                else:
                    raise confluent_kafka.KafkaException(msg.error())
            if msg.offset() > boundary_watermarks[msg.partition()][1]:  # type: ignore[index, operator]
                finished_parts[msg.partition()] = True  # type: ignore[index]
                if all(finished_parts):
                    break
                continue

            ctr += 1
            if ctr % 100000 == 0:
                logger.debug('consume_bounded has yielded %s messages so far from topic %s', ctr, topic_name)

            yield msg

        consumer.close()

    def create_topic(self, topic_name: str, partition_count: int, replication_factor: Optional[int] = None,
                     extra_config: Optional[Dict[str, str | int]] = None) -> None:
        if self._disable_writing:
            return

        if not replication_factor:
            if self._cluster_metadata.brokers is None:
                raise Exception('Unexpected state: no brokers metadata')
            replication_factor = min(len(self._cluster_metadata.brokers), 3)

        extra_config = extra_config or {}
        topic_config = {**{'cleanup.policy': 'compact'}, **extra_config}
        topic_config_str = {k: str(v) for k, v in topic_config.items()}

        logger.info('Creating Kafka topic "%s" with %s partitions, replication factor %s, and config: %s', topic_name,
                    partition_count, replication_factor, json.dumps(topic_config_str))
        topic = confluent_kafka.admin.NewTopic(topic_name, partition_count, replication_factor, config=topic_config_str)
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
            restype=confluent_kafka.admin.ConfigResource.Type.TOPIC, name=topic_name)  # type: ignore[attr-defined]
        result = self._admin.describe_configs([resource])
        return result[resource].result()

    def _refresh_cluster_metadata(self) -> None:
        self._cluster_metadata = self._get_cluster_metadata()

    def _get_cluster_metadata(self) -> confluent_kafka.admin.ClusterMetadata:
        if self._use_oauth:
            self._admin.poll(constants.KAFKA_OAUTH_CB_POLL_TIMEOUT)  # In case oauth token refresh is needed
        metadata = self._admin.list_topics(timeout=constants.KAFKA_REQUEST_TIMEOUT_SECS)
        if metadata is None:
            raise Exception(f'Cluster metadata request to Kafka timed out')
        return metadata

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
