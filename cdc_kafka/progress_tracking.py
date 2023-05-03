import collections
import datetime
import logging
from typing import Union, Dict, Tuple, Any, Optional

from . import constants
from .change_index import ChangeIndex

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .kafka import KafkaClient
    import confluent_kafka

logger = logging.getLogger(__name__)

PROGRESS_TRACKING_SCHEMA_VERSION = '2'
PROGRESS_TRACKING_AVRO_KEY_SCHEMA = {
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
}
PROGRESS_TRACKING_AVRO_VALUE_SCHEMA = {
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
        # Next two are null in the case of "heartbeat" change progress entries for slowly-updated tables
        {
            "name": "last_ack_partition",
            "type": ["null", "int"]
        },
        {
            "name": "last_ack_offset",
            "type": ["null", "long"]
        },
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
}


class ProgressEntry(object):
    def __init__(self, message: 'confluent_kafka.Message' = None, progress_kind: str = None, topic_name: str = None,
                 source_table_name: str = None, change_table_name: str = None, last_ack_partition: int = None,
                 last_ack_offset: int = None, snapshot_index: Dict[str, Union[str, int]] = None,
                 change_index: ChangeIndex = None) -> None:
        have_separate_params: bool = all((progress_kind, topic_name, source_table_name, change_table_name,
                                         (snapshot_index or change_index)))

        # noinspection PyArgumentList
        if bool(message and message.value()) == have_separate_params:
            raise Exception('Please pass either the `message` parameter OR the other parameters when initializing '
                            'a ProgressEntry object.')

        if message:
            # noinspection PyTypeChecker,PyArgumentList
            k, v = dict(message.key()), dict(message.value())

            if k['progress_kind'] not in (constants.CHANGE_ROWS_KIND, constants.SNAPSHOT_ROWS_KIND):
                raise Exception(f"Unrecognized progress kind from message: {k['progress_kind']}")

            self.progress_kind: str = k['progress_kind']
            self.topic_name: str = k['topic_name']
            self.source_table_name: str = v['source_table_name']
            self.change_table_name: str = v['change_table_name']
            self.last_ack_partition: int = v['last_ack_partition']
            self.last_ack_offset: int = v['last_ack_offset']
            self.snapshot_index: Optional[Dict[str, Union[str, int]]] = None
            self.change_index: Optional[ChangeIndex] = None

            if self.progress_kind == constants.SNAPSHOT_ROWS_KIND:
                self.snapshot_index = v['last_ack_position']['key_fields']
            else:
                self.change_index = ChangeIndex.from_avro_ready_dict(v['last_ack_position'])

        else:
            if progress_kind not in (constants.CHANGE_ROWS_KIND, constants.SNAPSHOT_ROWS_KIND):
                raise Exception(f'Unrecognized progress kind: {progress_kind}')

            self.progress_kind: str = progress_kind
            self.topic_name: str = topic_name
            self.source_table_name: str = source_table_name
            self.change_table_name: str = change_table_name
            self.last_ack_partition: int = last_ack_partition
            self.last_ack_offset: int = last_ack_offset
            self.snapshot_index: Optional[Dict[str, Union[str, int]]] = snapshot_index
            self.change_index: Optional[ChangeIndex] = change_index

    @property
    def key(self) -> Dict[str, str]:
        return {
            'topic_name': self.topic_name,
            'progress_kind': self.progress_kind
        }

    @property
    def value(self) -> Dict[str, Any]:
        if self.progress_kind == constants.CHANGE_ROWS_KIND:
            pos = self.change_index.to_avro_ready_dict()
        else:
            pos = {'key_fields': self.snapshot_index}
        return {
            'source_table_name': self.source_table_name,
            'change_table_name': self.change_table_name,
            'last_ack_partition': self.last_ack_partition,
            'last_ack_offset': self.last_ack_offset,
            'last_ack_position':  pos
        }

    @property
    def is_heartbeat(self):
        return self.last_ack_offset is None and self.last_ack_partition is None

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
        self._progress_messages_awaiting_commit: Dict[Tuple, Dict[int, Tuple[int, ProgressEntry]]] = \
            collections.defaultdict(dict)
        self._commit_order_enforcer: Dict[Tuple[str, str], int] = {}
        self._last_recorded_progress_by_topic: Dict[str, ProgressEntry] = {}

        ProgressTracker._instance = self

    def __enter__(self) -> 'ProgressTracker':
        return self

    def __exit__(self, exc_type, value, traceback) -> None:
        logger.info("Committing final progress...")
        self.commit_progress(final=True)
        logger.info("Done.")

    def get_last_recorded_progress_for_topic(self, topic_name: str) -> Optional[ProgressEntry]:
        return self._last_recorded_progress_by_topic.get(topic_name)

    # This emits a "no-op" constants.CHANGE_ROWS_KIND record to keep the progressed LSN position (that would be read
    # when this process starts) up-to-date. This will only be called for tables with very low change volume, after a
    # while has passed since the last change row was seen. This helps avoid warnings/halts due to LSN gaps seen at
    # startup if a change table is empty, e.g. due to CDC log truncation.
    def emit_changes_progress_heartbeat(self, topic_name: str, index: ChangeIndex) -> None:
        if (topic_name, constants.CHANGE_ROWS_KIND) in self._progress_messages_awaiting_commit:
            logger.info('Skipping emit_changes_progress_heartbeat for %s because a progress message is already '
                        'awaiting commit.', topic_name)
            return

        progress_entry = ProgressEntry(
            progress_kind=constants.CHANGE_ROWS_KIND,
            topic_name=topic_name,
            source_table_name=self._topic_to_source_table_map[topic_name],
            change_table_name=self._topic_to_change_table_map[topic_name],
            last_ack_partition=None,
            last_ack_offset=None,
            snapshot_index=None,
            change_index=index
        )
        self._kafka_client.produce(self._progress_topic_name, progress_entry.key, self._progress_key_schema_id,
                                   progress_entry.value, self._progress_value_schema_id,
                                   constants.HEARTBEAT_PROGRESS_MESSAGE)
        self._last_recorded_progress_by_topic[topic_name] = progress_entry

    def record_snapshot_completion(self, topic_name: str) -> None:
        self.commit_progress(final=True)

        progress_entry = ProgressEntry(
            progress_kind=constants.SNAPSHOT_ROWS_KIND,
            topic_name=topic_name,
            source_table_name=self._topic_to_source_table_map[topic_name],
            change_table_name=self._topic_to_change_table_map[topic_name],
            last_ack_partition=None,
            last_ack_offset=None,
            snapshot_index=constants.SNAPSHOT_COMPLETION_SENTINEL,
            change_index=None
        )

        self._kafka_client.produce(self._progress_topic_name, progress_entry.key, self._progress_key_schema_id,
                                   progress_entry.value, self._progress_value_schema_id,
                                   constants.SNAPSHOT_PROGRESS_MESSAGE)

    def kafka_delivery_callback(self, message_type: str, message: 'confluent_kafka.Message',
                                original_key: Dict[str, Any], original_value: Dict[str, Any],
                                produce_sequence: int, **_) -> None:
        if message_type == constants.SINGLE_TABLE_SNAPSHOT_MESSAGE:
            progress_entry = ProgressEntry(
                progress_kind=constants.SNAPSHOT_ROWS_KIND,
                topic_name=message.topic(),
                source_table_name=self._topic_to_source_table_map[message.topic()],
                change_table_name=self._topic_to_change_table_map[message.topic()],
                last_ack_partition=message.partition(),
                last_ack_offset=message.offset(),
                snapshot_index=original_key,
                change_index=None
            )
        else:
            progress_entry = ProgressEntry(
                progress_kind=constants.CHANGE_ROWS_KIND,
                topic_name=message.topic(),
                source_table_name=self._topic_to_source_table_map[message.topic()],
                change_table_name=self._topic_to_change_table_map[message.topic()],
                last_ack_partition=message.partition(),
                last_ack_offset=message.offset(),
                snapshot_index=None,
                change_index=ChangeIndex.from_avro_ready_dict(original_value)
            )

        key = (message.topic(), progress_entry.progress_kind)
        self._progress_messages_awaiting_commit[key][message.partition()] = (produce_sequence, progress_entry)

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

            curr_entry = ProgressEntry(message=progress_msg)
            prior_entry = result.get(result_key)
            if prior_entry and prior_entry.progress_kind == constants.CHANGE_ROWS_KIND and \
                    prior_entry.change_index > curr_entry.change_index:
                prior_message = messages[result_key]
                logger.error(
                    'WARNING: Progress topic %s contains unordered entries for %s! Prior: p%s:o%s (%s), acked p%s:o%s, '
                    'pos %s; Current: p%s:o%s (%s), acked p%s:o%s, pos %s', self._progress_topic_name, result_key,
                    prior_message.partition(), prior_message.offset(),
                    datetime.datetime.fromtimestamp(prior_message.timestamp()[1] / 1000),
                    '-' if prior_entry.last_ack_partition is None else prior_entry.last_ack_partition,
                    prior_entry.last_ack_offset or 'HEART',
                    prior_entry.change_index, progress_msg.partition(), progress_msg.offset(),
                    datetime.datetime.fromtimestamp(progress_msg.timestamp()[1] / 1000),
                    '-' if curr_entry.last_ack_partition is None else curr_entry.last_ack_partition,
                    curr_entry.last_ack_offset or 'HEART',
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

    def commit_progress(self, final: bool = False) -> None:
        # this will trigger calls to the delivery callbacks which will update progress_messages_awaiting_commit
        flushed_all = self._kafka_client.flush(final)

        for (topic, kind), progress_by_partition in self._progress_messages_awaiting_commit.items():
            commit_sequence, commit_partition = None, None
            if (not flushed_all) and len(progress_by_partition) < self._kafka_client.get_topic_partition_count(topic):
                # not safe to commit anything if not all partitions have acked since there may still be some
                # less-progressed messages in flight for the non-acked partition:
                continue
            for partition, (produce_sequence, _) in progress_by_partition.items():
                if flushed_all:
                    if commit_sequence is None or produce_sequence > commit_sequence:
                        commit_sequence = produce_sequence
                        commit_partition = partition
                else:
                    if commit_sequence is None or produce_sequence < commit_sequence:
                        commit_sequence = produce_sequence
                        commit_partition = partition

            if commit_partition is not None:
                produce_sequence, progress_entry = progress_by_partition.pop(commit_partition)
                prior_sequence = self._commit_order_enforcer.get((kind, topic))
                if prior_sequence is not None and produce_sequence < prior_sequence:
                    raise Exception('Committing progress out of order. There is a bug. Fix it.')
                self._commit_order_enforcer[(kind, topic)] = produce_sequence
                logger.debug('Producing progress: (flushed_all: %s) %s', flushed_all, progress_entry)
                message_type = constants.SNAPSHOT_PROGRESS_MESSAGE if kind == constants.SNAPSHOT_ROWS_KIND \
                    else constants.CHANGE_PROGRESS_MESSAGE
                self._kafka_client.produce(self._progress_topic_name, progress_entry.key, self._progress_key_schema_id,
                                           progress_entry.value, self._progress_value_schema_id, message_type)
                if message_type == constants.CHANGE_PROGRESS_MESSAGE:
                    self._last_recorded_progress_by_topic[topic] = progress_entry

        if final:
            if not self._kafka_client.flush(True):
                logger.error('Unable to complete final flush of Kafka producer queue.')

        if flushed_all:
            self._progress_messages_awaiting_commit = collections.defaultdict(dict)
