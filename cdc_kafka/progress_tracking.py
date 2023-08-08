import datetime
import json
import logging
from typing import Dict, Tuple, Any, Optional, Mapping, TypeVar, Type

import confluent_kafka.avro

from . import constants
from .change_index import ChangeIndex

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .kafka import KafkaClient
    import confluent_kafka

logger = logging.getLogger(__name__)

PROGRESS_TRACKING_SCHEMA_VERSION = '2'
PROGRESS_TRACKING_AVRO_KEY_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{constants.AVRO_SCHEMA_NAMESPACE}__progress_tracking_v{PROGRESS_TRACKING_SCHEMA_VERSION}__key",
    "namespace": constants.AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "topic_name",
            "type": "string"
        },
        {
            "name": "progress_kind",
            "type": {
                "type": "enum",
                "name": "progress_kind",
                "symbols": [
                    constants.CHANGE_ROWS_KIND,
                    constants.SNAPSHOT_ROWS_KIND
                ]
            }
        }
    ]
}))
PROGRESS_TRACKING_AVRO_VALUE_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{constants.AVRO_SCHEMA_NAMESPACE}__progress_tracking_v{PROGRESS_TRACKING_SCHEMA_VERSION}__value",
    "namespace": constants.AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "source_table_name",
            "type": "string"
        },
        {
            "name": "change_table_name",
            "type": "string"
        },
        # ------------------------------------------------------------------------------------------------
        # These next two are defunct/deprecated as of v4 but remain here to ease the upgrade transition
        # for anyone with existing progress recorded by earlier versions:
        {
            "name": "last_ack_partition",
            "type": ["null", "int"]
        },
        {
            "name": "last_ack_offset",
            "type": ["null", "long"]
        },
        # ------------------------------------------------------------------------------------------------
        {
            "name": "last_ack_position",
            "type": [
                {
                    "type": "record",
                    "name": f"{constants.CHANGE_ROWS_KIND}_progress",
                    "namespace": constants.AVRO_SCHEMA_NAMESPACE,
                    "fields": [
                        {
                            "name": constants.LSN_NAME,
                            "type": "string",
                        },
                        {
                            "name": constants.SEQVAL_NAME,
                            "type": "string",
                        },
                        {
                            "name": constants.OPERATION_NAME,
                            "type": {
                                "type": "enum",
                                "name": constants.OPERATION_NAME,
                                "symbols": list(constants.CDC_OPERATION_NAME_TO_ID.keys())
                            }
                        }
                    ]
                },
                {
                    "type": "record",
                    "name": f"{constants.SNAPSHOT_ROWS_KIND}_progress",
                    "namespace": constants.AVRO_SCHEMA_NAMESPACE,
                    "fields": [
                        {
                            "name": "key_fields",
                            "type": {
                                "type": "map",
                                "values": ["string", "long"]
                            }
                        }
                    ]
                }
            ]
        }
    ]
}))

ProgressEntryType = TypeVar('ProgressEntryType', bound='ProgressEntry')


class ProgressEntry(object):
    @classmethod
    def from_message(cls: Type[ProgressEntryType], message: 'confluent_kafka.Message') -> ProgressEntryType:
        # noinspection PyTypeChecker,PyArgumentList
        k, v = dict(message.key()), dict(message.value())
        kind: str = k['progress_kind']

        if kind not in (constants.CHANGE_ROWS_KIND, constants.SNAPSHOT_ROWS_KIND):
            raise Exception(f"Unrecognized progress kind from message: {kind}")

        if kind == constants.SNAPSHOT_ROWS_KIND:
            return cls(kind, k['topic_name'], v['source_table_name'], v['change_table_name'],
                       v['last_ack_position']['key_fields'], None)

        else:
            return cls(kind, k['topic_name'], v['source_table_name'], v['change_table_name'],
                       None, ChangeIndex.from_avro_ready_dict(v['last_ack_position']))

    def __init__(self, progress_kind: str, topic_name: str, source_table_name: str, change_table_name: str,
                 snapshot_index: Optional[Mapping[str, str | int]] = None,
                 change_index: Optional[ChangeIndex] = None) -> None:
        if progress_kind not in (constants.CHANGE_ROWS_KIND, constants.SNAPSHOT_ROWS_KIND):
            raise Exception(f'Unrecognized progress kind: {progress_kind}')

        self.progress_kind: str = progress_kind
        self.topic_name: str = topic_name
        self.source_table_name: str = source_table_name
        self.change_table_name: str = change_table_name
        self.snapshot_index: Optional[Mapping[str, str | int]] = snapshot_index
        self.change_index: Optional[ChangeIndex] = change_index

    @property
    def key(self) -> Dict[str, str]:
        return {
            'topic_name': self.topic_name,
            'progress_kind': self.progress_kind
        }

    @property
    def value(self) -> Dict[str, Any]:
        pos: Dict[str, Any]
        if self.change_index:
            pos = self.change_index.to_avro_ready_dict()
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

    def __init__(self, kafka_client: 'KafkaClient', progress_topic_name: str,
                 topic_to_source_table_map: Dict[str, str],
                 topic_to_change_table_map: Dict[str, str]) -> None:
        if ProgressTracker._instance is not None:
            raise Exception('ProgressTracker class should be used as a singleton.')

        self._kafka_client: 'KafkaClient' = kafka_client
        self._progress_topic_name: str = progress_topic_name
        self._topic_to_source_table_map: Dict[str, str] = topic_to_source_table_map
        self._topic_to_change_table_map: Dict[str, str] = topic_to_change_table_map
        self._progress_key_schema_id, self._progress_value_schema_id = kafka_client.register_schemas(
            progress_topic_name, PROGRESS_TRACKING_AVRO_KEY_SCHEMA, PROGRESS_TRACKING_AVRO_VALUE_SCHEMA)
        self._last_recorded_progress_by_topic: Dict[str, ProgressEntry] = {}

        ProgressTracker._instance = self

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

        self._kafka_client.produce(
            topic=self._progress_topic_name,
            key=progress_entry.key,
            key_schema_id=self._progress_key_schema_id,
            value=progress_entry.value,
            value_schema_id=self._progress_value_schema_id,
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

        self._kafka_client.produce(
            topic=self._progress_topic_name,
            key=progress_entry.key,
            key_schema_id=self._progress_key_schema_id,
            value=progress_entry.value,
            value_schema_id=self._progress_value_schema_id,
            message_type=constants.SNAPSHOT_PROGRESS_MESSAGE
        )

    def get_prior_progress_or_create_progress_topic(self) -> Dict[Tuple[str, str], ProgressEntry]:
        if self._kafka_client.get_topic_partition_count(self._progress_topic_name) is None:
            logger.warning('No existing progress storage topic found; creating topic %s', self._progress_topic_name)

            # log.segment.bytes set to 16 MB. Compaction will not run until the next log segment rolls, so we set this
            # a bit low (the default is 1 GB!) to prevent having to read too much from the topic on process startup:
            self._kafka_client.create_topic(self._progress_topic_name, 1, extra_config={"segment.bytes": 16777216})
            return {}
        return self.get_prior_progress()

    # the keys in the returned dictionary are tuples of (topic_name, progress_kind)
    def get_prior_progress(self) -> Dict[Tuple[str, str], ProgressEntry]:
        result: Dict[Tuple[str, str], ProgressEntry] = {}
        messages: Dict[Tuple[str, str], confluent_kafka.Message] = {}

        progress_msg_ctr = 0
        for progress_msg in self._kafka_client.consume_all(self._progress_topic_name):
            progress_msg_ctr += 1
            # noinspection PyTypeChecker
            msg_key = dict(progress_msg.key())
            result_key = (msg_key['topic_name'], msg_key['progress_kind'])

            # noinspection PyArgumentList
            if progress_msg.value() is None:
                if result_key in result:
                    del result[result_key]
                continue

            curr_entry = ProgressEntry.from_message(message=progress_msg)
            prior_entry = result.get(result_key)
            if (prior_entry and prior_entry.change_index and curr_entry and curr_entry.change_index and
                    prior_entry.change_index > curr_entry.change_index):
                prior_message = messages[result_key]
                logger.error(
                    'WARNING: Progress topic %s contains unordered entries for %s! Prior: p%s:o%s (%s), '
                    'pos %s; Current: p%s:o%s (%s), pos %s', self._progress_topic_name, result_key,
                    prior_message.partition(), prior_message.offset(),
                    datetime.datetime.fromtimestamp(prior_message.timestamp()[1] / 1000),
                    prior_entry.change_index, progress_msg.partition(), progress_msg.offset(),
                    datetime.datetime.fromtimestamp(progress_msg.timestamp()[1] / 1000),
                    curr_entry.change_index)
            result[result_key] = curr_entry  # last read for a given key will win
            messages[result_key] = progress_msg

        logger.info('Read %s prior progress messages from Kafka topic %s', progress_msg_ctr, self._progress_topic_name)
        return result

    def reset_progress(self, topic_name: str, kind_to_reset: str) -> None:
        # Produce messages with empty values to "delete" them from Kafka
        matched = False

        if kind_to_reset in (constants.CHANGE_ROWS_KIND, constants.ALL_PROGRESS_KINDS):
            key = {
                'topic_name': topic_name,
                'progress_kind': constants.CHANGE_ROWS_KIND,
            }
            self._kafka_client.produce(self._progress_topic_name, key, self._progress_key_schema_id, None,
                                       self._progress_value_schema_id, constants.PROGRESS_DELETION_TOMBSTONE_MESSAGE)
            logger.info('Deleted existing change rows progress records for topic %s.', topic_name)
            matched = True

        if kind_to_reset in (constants.SNAPSHOT_ROWS_KIND, constants.ALL_PROGRESS_KINDS):
            key = {
                'topic_name': topic_name,
                'progress_kind': constants.SNAPSHOT_ROWS_KIND,
            }
            self._kafka_client.produce(self._progress_topic_name, key, self._progress_key_schema_id, None,
                                       self._progress_value_schema_id, constants.PROGRESS_DELETION_TOMBSTONE_MESSAGE)
            logger.info('Deleted existing snapshot progress records for topic %s.', topic_name)
            matched = True

        if not matched:
            raise Exception(f'Function reset_progress received unrecognized argument "{kind_to_reset}" for '
                            f'kind_to_reset.')
