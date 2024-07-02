import datetime
import logging
from typing import Dict, Tuple, Any, Optional, Mapping, TypeVar, Type, TYPE_CHECKING

from . import constants, helpers, tracked_tables
from .change_index import ChangeIndex
from .serializers import SerializerAbstract, DeserializedMessage

if TYPE_CHECKING:
    from .kafka import KafkaClient
    import confluent_kafka

logger = logging.getLogger(__name__)


ProgressEntryType = TypeVar('ProgressEntryType', bound='ProgressEntry')


class ProgressEntry(object):
    @classmethod
    def from_message(cls: Type[ProgressEntryType], message: DeserializedMessage) -> ProgressEntryType:
        # noinspection PyTypeChecker,PyArgumentList
        k, v = message.key_dict, message.value_dict

        if k is None or v is None:
            raise Exception("Malformed message received by ProgressEntry.from_message")

        kind: str = k['progress_kind']

        if kind not in (constants.CHANGE_ROWS_KIND, constants.SNAPSHOT_ROWS_KIND):
            raise Exception(f"Unrecognized progress kind from message: {kind}")

        msg_coordinates = helpers.format_coordinates(message.raw_msg)

        if kind == constants.SNAPSHOT_ROWS_KIND:
            return cls(kind, k['topic_name'], v['source_table_name'], v['change_table_name'],
                       v['last_ack_position']['key_fields'], None, msg_coordinates)

        else:
            return cls(kind, k['topic_name'], v['source_table_name'], v['change_table_name'],
                       None, ChangeIndex.from_dict(v['last_ack_position']), msg_coordinates)

    def __init__(self, progress_kind: str, topic_name: str, source_table_name: str, change_table_name: str,
                 snapshot_index: Optional[Mapping[str, str | int]] = None,
                 change_index: Optional[ChangeIndex] = None, progress_msg_coordinates: Optional[str] = None) -> None:
        if progress_kind not in (constants.CHANGE_ROWS_KIND, constants.SNAPSHOT_ROWS_KIND):
            raise Exception(f'Unrecognized progress kind: {progress_kind}')

        self.progress_kind: str = progress_kind
        self.topic_name: str = topic_name
        self.source_table_name: str = source_table_name
        self.change_table_name: str = change_table_name
        self.snapshot_index: Optional[Mapping[str, str | int]] = snapshot_index
        self.change_index: Optional[ChangeIndex] = change_index
        self.progress_msg_coordinates: Optional[str] = progress_msg_coordinates

    @property
    def key(self) -> Dict[str, str]:
        return {
            'topic_name': self.topic_name,
            'progress_kind': self.progress_kind
        }

    @property
    def value(self) -> Optional[Dict[str, Any]]:
        if not (self.change_index or self.snapshot_index):
            return None
        pos: Dict[str, Any]
        if self.change_index:
            pos = self.change_index.as_dict()
        else:
            pos = {'key_fields': self.snapshot_index}
        return {
            'source_table_name': self.source_table_name,
            'change_table_name': self.change_table_name,
            'last_ack_position': pos
        }

    def __repr__(self) -> str:
        progress = self.snapshot_index if self.progress_kind == constants.SNAPSHOT_ROWS_KIND else self.change_index
        return f'ProgressEntry for {self.topic_name}; {self.progress_kind} progress: {progress}'


class ProgressTracker(object):
    _instance = None

    def __init__(self, kafka_client: 'KafkaClient', serializer: SerializerAbstract, progress_topic_name: str,
                 process_hostname: str, snapshot_logging_topic_name: Optional[str] = None) -> None:
        if ProgressTracker._instance is not None:
            raise Exception('ProgressTracker class should be used as a singleton.')

        self._kafka_client: 'KafkaClient' = kafka_client
        self._serializer: SerializerAbstract = serializer
        self._progress_topic_name: str = progress_topic_name
        self._process_hostname: str = process_hostname
        self._snapshot_logging_topic_name: Optional[str] = snapshot_logging_topic_name
        self._last_recorded_progress_by_topic: Dict[str, ProgressEntry] = {}
        self._topic_to_source_table_map: Dict[str, str] = {}
        self._topic_to_change_table_map: Dict[str, str] = {}

        ProgressTracker._instance = self

    def register_table(self, table: 'tracked_tables.TrackedTable') -> None:
        self._topic_to_source_table_map[table.topic_name] = table.fq_name
        self._topic_to_change_table_map[table.topic_name] = helpers.get_fq_change_table_name(
            table.capture_instance_name)

    def get_last_recorded_progress_for_topic(self, topic_name: str) -> Optional[ProgressEntry]:
        return self._last_recorded_progress_by_topic.get(topic_name)

    def record_changes_progress(self, topic_name: str, change_index: ChangeIndex) -> None:
        progress_entry = ProgressEntry(
            progress_kind=constants.CHANGE_ROWS_KIND,
            topic_name=topic_name,
            source_table_name=self._topic_to_source_table_map[topic_name],
            change_table_name=self._topic_to_change_table_map[topic_name],
            change_index=change_index
        )

        key, value = self._serializer.serialize_progress_tracking_message(progress_entry)

        self._kafka_client.produce(
            topic=self._progress_topic_name,
            key=key,
            value=value,
            message_type=constants.CHANGE_PROGRESS_MESSAGE
        )

        self._last_recorded_progress_by_topic[topic_name] = progress_entry

    def record_snapshot_progress(self, topic_name: str, snapshot_index: Mapping[str, str | int]) -> None:
        progress_entry = ProgressEntry(
            progress_kind=constants.SNAPSHOT_ROWS_KIND,
            topic_name=topic_name,
            source_table_name=self._topic_to_source_table_map[topic_name],
            change_table_name=self._topic_to_change_table_map[topic_name],
            snapshot_index=snapshot_index
        )

        key, value = self._serializer.serialize_progress_tracking_message(progress_entry)

        self._kafka_client.produce(
            topic=self._progress_topic_name,
            key=key,
            value=value,
            message_type=constants.SNAPSHOT_PROGRESS_MESSAGE
        )

    def _log_snapshot_event(self, topic_name: str, table_name: str, action: str,
                            event_time: Optional[datetime.datetime] = None,
                            starting_snapshot_index: Optional[Mapping[str, str | int]] = None,
                            ending_snapshot_index: Optional[Mapping[str, str | int]] = None) -> None:
        if self._snapshot_logging_topic_name is None:
            return

        low_wms: Dict[str, int] = {}
        high_wms: Dict[str, int] = {}
        for partition, (lo_wm, hi_wm) in enumerate(self._kafka_client.get_topic_watermarks([topic_name])[topic_name]):
            low_wms[str(partition)] = lo_wm
            high_wms[str(partition)] = hi_wm

        event_time_iso = event_time.isoformat() if event_time is not None \
            else helpers.naive_utcnow().isoformat()

        msg = {
            "action": action,
            "ending_snapshot_index": ending_snapshot_index,
            "event_time_utc": event_time_iso,
            "partition_watermarks_high": high_wms,
            "partition_watermarks_low": low_wms,
            "process_hostname": self._process_hostname,
            "starting_snapshot_index": starting_snapshot_index,
            "table_name": table_name,
            "topic_name": topic_name
        }

        logger.debug('Logging snapshot event: %s', msg)
        _, value = self._serializer.serialize_snapshot_logging_message(msg)
        self._kafka_client.produce(
            topic=self._snapshot_logging_topic_name,
            key=None,
            value=value,
            message_type=constants.SNAPSHOT_LOGGING_MESSAGE
        )

    def log_snapshot_started(self, topic_name: str, table_name: str,
                             starting_snapshot_index: Mapping[str, str | int]) -> None:
        return self._log_snapshot_event(topic_name, table_name, constants.SNAPSHOT_LOG_ACTION_STARTED,
                                        starting_snapshot_index=starting_snapshot_index)

    def log_snapshot_resumed(self, topic_name: str, table_name: str,
                             starting_snapshot_index: Mapping[str, str | int]) -> None:
        return self._log_snapshot_event(topic_name, table_name, constants.SNAPSHOT_LOG_ACTION_RESUMED,
                                        starting_snapshot_index=starting_snapshot_index)

    def log_snapshot_completed(self, topic_name: str, table_name: str, event_time: datetime.datetime,
                               ending_snapshot_index: Mapping[str, str | int]) -> None:
        return self._log_snapshot_event(topic_name, table_name, constants.SNAPSHOT_LOG_ACTION_COMPLETED,
                                        event_time=event_time, ending_snapshot_index=ending_snapshot_index)

    def log_snapshot_progress_reset(self, topic_name: str, table_name: str, is_auto_reset: bool,
                                    prior_progress_snapshot_index: Optional[Mapping[str, str | int]]) -> None:
        action = constants.SNAPSHOT_LOG_ACTION_RESET_AUTO if is_auto_reset \
            else constants.SNAPSHOT_LOG_ACTION_RESET_MANUAL
        return self._log_snapshot_event(topic_name, table_name, action,
                                        ending_snapshot_index=prior_progress_snapshot_index)

    def get_prior_progress_or_create_progress_topic(self) -> Dict[Tuple[str, str], ProgressEntry]:
        if not self._kafka_client.get_topic_partition_count(self._progress_topic_name):
            logger.warning('No existing progress storage topic found; creating topic %s', self._progress_topic_name)

            # log.segment.bytes set to 16 MB. Compaction will not run until the next log segment rolls, so we set this
            # a bit low (the default is 1 GB!) to prevent having to read too much from the topic on process startup:
            self._kafka_client.create_topic(self._progress_topic_name, 1,
                                            extra_config={"segment.bytes": 16 * 1024 * 1024})
            return {}
        return self.get_prior_progress()

    def maybe_create_snapshot_logging_topic(self) -> None:
        if (self._snapshot_logging_topic_name and not
                self._kafka_client.get_topic_partition_count(self._snapshot_logging_topic_name)):
            logger.warning('No existing snapshot logging topic found; creating topic %s',
                           self._snapshot_logging_topic_name)
            self._kafka_client.create_topic(self._snapshot_logging_topic_name, 1,
                                            extra_config={'cleanup.policy': 'delete',
                                                          'retention.ms': 365 * 24 * 60 * 60 * 1000})  # 1 year

    # the keys in the returned dictionary are tuples of (topic_name, progress_kind)
    def get_prior_progress(self) -> Dict[Tuple[str, str], ProgressEntry]:
        raw_msgs: Dict[bytes | str | None, confluent_kafka.Message] = {}

        progress_msg_ctr = 0
        for progress_msg in self._kafka_client.consume_all(self._progress_topic_name):
            progress_msg_ctr += 1
            # noinspection PyArgumentList
            if progress_msg.value() is None:
                if progress_msg.key() is not None and progress_msg.key() in raw_msgs:
                    del raw_msgs[progress_msg.key()]
                continue
            raw_msgs[progress_msg.key()] = progress_msg

        logger.info('Read %s prior progress messages from Kafka topic %s', progress_msg_ctr, self._progress_topic_name)

        result: Dict[Tuple[str, str], ProgressEntry] = {}
        for msg in raw_msgs.values():
            deser_msg = self._serializer.deserialize(msg)
            if deser_msg.key_dict is None:
                raise Exception('Unexpected state: None value from deserializing progress message key')
            result[(deser_msg.key_dict['topic_name'], deser_msg.key_dict['progress_kind'])] = \
                ProgressEntry.from_message(message=deser_msg)

        return result

    def reset_progress(self, topic_name: str, kind_to_reset: str, source_table_name: str, is_auto_reset: bool,
                       prior_progress_snapshot_index: Optional[Mapping[str, str | int]] = None) -> None:
        # Produce messages with empty values to "delete" them from Kafka
        matched = False

        if kind_to_reset in (constants.CHANGE_ROWS_KIND, constants.ALL_PROGRESS_KINDS):
            progress_entry = ProgressEntry(constants.CHANGE_ROWS_KIND, topic_name, '', '')
            key, _ = self._serializer.serialize_progress_tracking_message(progress_entry)
            self._kafka_client.produce(
                topic=self._progress_topic_name,
                key=key,
                value=None,
                message_type=constants.PROGRESS_DELETION_TOMBSTONE_MESSAGE
            )
            logger.info('Deleted existing change rows progress records for topic %s.', topic_name)
            matched = True

        if kind_to_reset in (constants.SNAPSHOT_ROWS_KIND, constants.ALL_PROGRESS_KINDS):
            progress_entry = ProgressEntry(constants.SNAPSHOT_ROWS_KIND, topic_name, '', '')
            key, _ = self._serializer.serialize_progress_tracking_message(progress_entry)
            self._kafka_client.produce(
                topic=self._progress_topic_name,
                key=key,
                value=None,
                message_type=constants.PROGRESS_DELETION_TOMBSTONE_MESSAGE
            )
            logger.info('Deleted existing snapshot progress records for topic %s.', topic_name)
            self.maybe_create_snapshot_logging_topic()
            self.log_snapshot_progress_reset(topic_name, source_table_name, is_auto_reset,
                                             prior_progress_snapshot_index)
            matched = True

        if not matched:
            raise Exception(f'Function reset_progress received unrecognized argument "{kind_to_reset}" for '
                            f'kind_to_reset.')
