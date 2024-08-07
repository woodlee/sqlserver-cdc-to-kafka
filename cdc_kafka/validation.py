import collections
import datetime
import functools
import json
import logging
from typing import List, Iterable, Dict, Tuple, Any, Optional, TYPE_CHECKING, Set, Mapping, Sequence
from uuid import UUID

from . import constants, change_index, kafka, helpers
from .serializers import SerializerAbstract, DeserializedMessage

if TYPE_CHECKING:
    from . import tracked_tables, progress_tracking

logger = logging.getLogger(__name__)


@functools.total_ordering
class SQLServerUUID(object):
    # implements UUID comparison the way SQL Server does it, so we can order them the same way

    def __init__(self, uuid: str | UUID) -> None:
        self.uuid = UUID(uuid) if isinstance(uuid, str) else uuid
        b = bytearray(self.uuid.bytes)
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15] = \
            b[10], b[11], b[12], b[13], b[14], b[15], b[8], b[9], b[7], b[6], b[5], b[4], b[3], b[2], b[1], b[0]
        self.sql_ordered_bytes = bytes(b)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SQLServerUUID):
            return NotImplemented
        return self.sql_ordered_bytes == other.sql_ordered_bytes

    def __lt__(self, other: 'SQLServerUUID') -> bool:
        return self.sql_ordered_bytes < other.sql_ordered_bytes

    def __hash__(self) -> int:
        return hash(str(self))

    def __repr__(self) -> str:
        return str(self.uuid).upper()


def extract_key_tuple(table: 'tracked_tables.TrackedTable', message: Mapping[str, Any]) -> Tuple[Any, ...]:
    key_bits: List[Any] = []
    for kf in table.key_fields:
        if kf.sql_type_name == 'uniqueidentifier':
            key_bits.append(SQLServerUUID(str(message[kf.name])))
        elif kf.sql_type_name in constants.SQL_STRING_TYPES:
            key_bits.append(str(message[kf.name]).casefold())
        else:
            key_bits.append(message[kf.name])
    return tuple(key_bits)


class TableMessagesSummary(object):
    def __init__(self, table: 'tracked_tables.TrackedTable') -> None:
        self.table = table
        self.tombstone_count: int = 0
        self.all_deletes_in_topic: int = 0
        self.delete_count: int = 0
        self.insert_count: int = 0
        self.update_count: int = 0
        self.snapshot_count: int = 0
        self.unknown_operation_count: int = 0
        self.total_count: int = 0
        self.keys_seen_in_snapshots: Set[Tuple[Any, ...]] = set()
        self.deleted_keys: Set[Tuple[Any, ...]] = set()
        self.keys_seen_in_changes: Set[Tuple[Any, ...]] = set()
        self.min_change_index_seen: Optional[change_index.ChangeIndex] = None
        self.max_change_index_seen: Optional[change_index.ChangeIndex] = None
        self.max_change_index_seen_coordinates: Optional[str] = None
        self.latest_change_seen: Optional[datetime.datetime] = None
        self.change_index_order_regressions_count: int = 0
        self.min_snapshot_key_seen: Optional[Tuple[Any, ...]] = None
        self.max_snapshot_key_seen: Optional[Tuple[Any, ...]] = None
        self.snapshot_key_order_regressions_count: int = 0
        self.missing_offsets: int = 0

        self._last_processed_offset_by_partition: Dict[int, int] = {}
        self._last_change_index_seen_for_partition: Dict[int, change_index.ChangeIndex] = {}
        self._last_snapshot_key_seen_for_partition: Dict[int, Tuple[Any, ...]] = {}
        self._last_snapshot_coordinates_seen_for_partition: Dict[int, str] = {}

    def __repr__(self) -> str:
        return json.dumps({
            'table_name': self.table.fq_name,
            'tombstone_count': self.tombstone_count,
            'all_deletes_in_topic': self.all_deletes_in_topic,
            'delete_count': self.delete_count,
            'insert_count': self.insert_count,
            'update_count': self.update_count,
            'snapshot_count': self.snapshot_count,
            'unknown_operation_count': self.unknown_operation_count,
            'total_count': self.total_count,
            'unique_keys_seen_in_snapshots_count': len(self.keys_seen_in_snapshots),
            'unique_keys_seen_in_changes_count': len(self.keys_seen_in_changes),
            'deleted_keys_count': len(self.deleted_keys),
            'min_change_index_seen': str(self.min_change_index_seen) if self.min_change_index_seen else None,
            'max_change_index_seen': str(self.max_change_index_seen) if self.max_change_index_seen else None,
            'latest_change_seen': self.latest_change_seen.isoformat() if self.latest_change_seen else None,
            'change_index_order_regressions_count': self.change_index_order_regressions_count,
            'min_snapshot_key_seen': str(self.min_snapshot_key_seen) if self.min_snapshot_key_seen else None,
            'max_snapshot_key_seen': str(self.max_snapshot_key_seen) if self.max_snapshot_key_seen else None,
            'snapshot_key_order_regressions_count': self.snapshot_key_order_regressions_count,
            'missing_offsets': self.missing_offsets,
        })

    def process_message(self, message: DeserializedMessage) -> None:
        self.total_count += 1
        raw_partition = message.raw_msg.partition()
        if raw_partition is None:
            raise Exception('Unexpected state: None value for message partition.')
        else:
            partition: int = raw_partition

        raw_offset = message.raw_msg.offset()
        if raw_offset is None:
            raise Exception('Unexpected state: None value for message offset.')
        else:
            offset: int = raw_offset

        if partition not in self._last_processed_offset_by_partition:
            self._last_processed_offset_by_partition[partition] = -1

        self.missing_offsets += (offset - self._last_processed_offset_by_partition[partition] - 1)
        self._last_processed_offset_by_partition[partition] = offset

        # noinspection PyArgumentList
        if not message.raw_msg.value():
            self.tombstone_count += 1
            return

        if message.value_dict is None:
            raise Exception('Unexpected state')

        key = extract_key_tuple(self.table, message.value_dict)
        coordinates = helpers.format_coordinates(message.raw_msg)
        operation_name = message.value_dict[constants.OPERATION_NAME]

        if operation_name == constants.SNAPSHOT_OPERATION_NAME:
            self.snapshot_count += 1
            self.keys_seen_in_snapshots.add(key)
            if self.min_snapshot_key_seen is None or key < self.min_snapshot_key_seen:
                self.min_snapshot_key_seen = key
            if self.max_snapshot_key_seen is None or key > self.max_snapshot_key_seen:
                self.max_snapshot_key_seen = key
            if partition in self._last_snapshot_key_seen_for_partition and \
                    self._last_snapshot_key_seen_for_partition[partition] < key:
                self.snapshot_key_order_regressions_count += 1
                logger.debug(
                    "Snapshot key order regression for %s: value %s at coordinates %s --> value %s at coordinates %s",
                    self.table.fq_name,
                    self._last_snapshot_key_seen_for_partition[partition],
                    self._last_snapshot_coordinates_seen_for_partition[partition],
                    key,
                    coordinates
                )
            self._last_snapshot_coordinates_seen_for_partition[partition] = coordinates
            self._last_snapshot_key_seen_for_partition[partition] = key
            return

        if operation_name == constants.DELETE_OPERATION_NAME:
            self.all_deletes_in_topic += 1
            self.deleted_keys.add(key)

        self.keys_seen_in_changes.add(key)

        msg_change_index = change_index.ChangeIndex.from_dict(message.value_dict)
        if msg_change_index.lsn < self.table.min_lsn:
            # the live change table has been truncated and no longer has this entry
            return

        if operation_name == constants.DELETE_OPERATION_NAME:
            self.delete_count += 1
        elif operation_name == constants.INSERT_OPERATION_NAME:
            self.insert_count += 1
        elif operation_name == constants.POST_UPDATE_OPERATION_NAME:
            self.update_count += 1
        else:
            self.unknown_operation_count += 1
            return

        change_idx = change_index.ChangeIndex.from_dict(message.value_dict)
        if self.min_change_index_seen is None or change_idx < self.min_change_index_seen:
            self.min_change_index_seen = change_idx
        if self.max_change_index_seen is None or change_idx > self.max_change_index_seen:
            self.max_change_index_seen = change_idx
            self.max_change_index_seen_coordinates = helpers.format_coordinates(message.raw_msg)
        if partition in self._last_change_index_seen_for_partition and \
                self._last_change_index_seen_for_partition[partition] > change_idx:
            self.change_index_order_regressions_count += 1
        self._last_change_index_seen_for_partition[partition] = change_idx
        event_time = datetime.datetime.fromisoformat(message.value_dict[constants.EVENT_TIME_NAME])
        if self.latest_change_seen is None or event_time > self.latest_change_seen:
            self.latest_change_seen = event_time
        return


class Validator(object):
    def __init__(self, kafka_client: 'kafka.KafkaClient', tables: Iterable['tracked_tables.TrackedTable'],
                 progress_tracker: 'progress_tracking.ProgressTracker', serializer: SerializerAbstract,
                 unified_topic_to_tables_map: Dict[str, List['tracked_tables.TrackedTable']]) -> None:
        self._kafka_client: 'kafka.KafkaClient' = kafka_client
        self._tables_by_name: Dict[str, 'tracked_tables.TrackedTable'] = {t.fq_name: t for t in tables}
        self._progress_tracker: 'progress_tracking.ProgressTracker' = progress_tracker
        self._serializer: SerializerAbstract = serializer
        self._unified_topic_to_tables_map: Dict[str, List['tracked_tables.TrackedTable']] = unified_topic_to_tables_map

    def run(self) -> None:
        watermarks_by_topic = self._kafka_client.get_topic_watermarks(
            [t.topic_name for t in self._tables_by_name.values()] +
            [t for t in self._unified_topic_to_tables_map.keys()]
        )
        progress = self._progress_tracker.get_prior_progress_or_create_progress_topic()
        summaries_by_unified_topic: Dict[str, Dict[str, Any]] = {}
        summaries_by_single_table: Dict[str, TableMessagesSummary] = {}

        for unified_topic_name, unified_topic_tables in self._unified_topic_to_tables_map.items():
            if self._kafka_client.get_topic_partition_count(unified_topic_name) != 1:
                logger.error('Unified topic validation currently cannot handle proper re-ordering of messages '
                             'when the topic has more than one partition. Skipping unified topic checks for '
                             'topic %s.', unified_topic_name)
                continue
            summaries_by_unified_topic[unified_topic_name] = self._process_unified_topic(
                unified_topic_name, unified_topic_tables, watermarks_by_topic[unified_topic_name])

        total_tables = len(self._tables_by_name)
        for source_topic_name, table in self._tables_by_name.items():
            logger.info('Processing table %s (%d/%d)', source_topic_name, len(summaries_by_single_table) + 1, total_tables)
            summaries_by_single_table[source_topic_name] = self._process_single_table_topic(
                table, watermarks_by_topic[table.topic_name])

        for source_topic_name, summary in summaries_by_single_table.items():
            table = self._tables_by_name[source_topic_name]
            failures, warnings, infos = [], [], []

            progress_entry = progress.get((table.topic_name, constants.SNAPSHOT_ROWS_KIND))
            snap_progress: Optional[Sequence[Any]] = None

            if progress_entry and progress_entry.snapshot_index:
                if progress_entry.snapshot_index == constants.SNAPSHOT_COMPLETION_SENTINEL:
                    snap_progress = tuple(constants.SNAPSHOT_COMPLETION_SENTINEL.keys())
                else:
                    snap_progress = extract_key_tuple(table, progress_entry.snapshot_index)

            changes_progress: Optional[progress_tracking.ProgressEntry] = progress.get(
                (table.topic_name, constants.CHANGE_ROWS_KIND))
            changes_progress_index: Optional[change_index.ChangeIndex] = None
            if changes_progress:
                changes_progress_index = changes_progress.change_index

            db_delete_rows, db_insert_rows, db_update_rows, db_source_row_counts = 0, 0, 0, 0
            if summary.max_change_index_seen:
                db_delete_rows, db_insert_rows, db_update_rows = table.get_change_table_counts(
                    summary.max_change_index_seen)

            topic_snapshot_key_count = len(summary.keys_seen_in_snapshots.difference(summary.deleted_keys))
            if topic_snapshot_key_count and summary.min_snapshot_key_seen and summary.max_snapshot_key_seen:
                low_raw_key, high_raw_key = [], []
                for k in summary.min_snapshot_key_seen:
                    if isinstance(k, SQLServerUUID):
                        low_raw_key.append(k.uuid)
                    else:
                        low_raw_key.append(k)
                for k in summary.max_snapshot_key_seen:
                    if isinstance(k, SQLServerUUID):
                        high_raw_key.append(k.uuid)
                    else:
                        high_raw_key.append(k)
                db_source_row_counts = table.get_source_table_count(tuple(low_raw_key), tuple(high_raw_key))
                change_keys_in_snapshot_range = {
                    x for x in summary.keys_seen_in_changes
                    if summary.min_snapshot_key_seen <= x <= summary.max_snapshot_key_seen
                }
                deleted_keys_in_snapshot_range = {
                    x for x in summary.deleted_keys
                    if summary.min_snapshot_key_seen <= x <= summary.max_snapshot_key_seen
                }
                expected = len(summary.keys_seen_in_snapshots.union(change_keys_in_snapshot_range) -
                               deleted_keys_in_snapshot_range)
                if db_source_row_counts != expected:
                    failures.append(f'DB source table has {db_source_row_counts} rows between keys {low_raw_key} '
                                    f'and {high_raw_key}, while {expected} messages were expected for that range '
                                    f'in the Kafka topic.')
                if len(summary.keys_seen_in_snapshots) != summary.snapshot_count:
                    warnings.append(
                        f'Count of unique keys in snapshot records ({len(summary.keys_seen_in_snapshots)}) not '
                        f'equal to total number of snapshot records ({summary.snapshot_count}). (This may be '
                        f'okay if more than one snapshot has been taken.)')
            else:
                infos.append(f'Skipping snapshot evaluations for sample lacking snapshot entries.')

            if summary.latest_change_seen is None:
                if db_delete_rows or db_insert_rows or db_update_rows:
                    failures.append('No change entries found!')
            elif (helpers.naive_utcnow() - summary.latest_change_seen) > datetime.timedelta(days=1):
                infos.append(f'Last change entry seen in Kafka was dated {summary.latest_change_seen}.')

            if not changes_progress_index:
                failures.append(f'No changes progress found. Last key found in topic was '
                                f'{summary.max_change_index_seen} @ {summary.max_change_index_seen_coordinates}')
            elif changes_progress_index.is_probably_heartbeat:
                if summary.max_change_index_seen and summary.max_change_index_seen > changes_progress_index:
                    failures.append(f'Changes progress mismatch. Last recorded heartbeat progress was '
                                    f'{changes_progress_index} but last key found in topic was '
                                    f'{summary.max_change_index_seen} @ {summary.max_change_index_seen_coordinates}')
            else:
                if summary.max_change_index_seen != changes_progress_index:
                    failures.append(f'Changes progress mismatch. Recorded progress index was {changes_progress_index} '
                                    f'but last key found in topic was {summary.max_change_index_seen} @ '
                                    f'{summary.max_change_index_seen_coordinates}')

            if snap_progress != tuple(constants.SNAPSHOT_COMPLETION_SENTINEL.keys()) and \
                    summary.min_snapshot_key_seen != snap_progress:
                failures.append(f'Snapshot progress mismatch. Recorded progress key was {snap_progress} but last key '
                                f'found in topic was {summary.min_snapshot_key_seen}')

            if summary.change_index_order_regressions_count:
                failures.append(f'Kafka topic contained {summary.change_index_order_regressions_count} regressions in '
                                f'change index ordering.')
            if summary.snapshot_key_order_regressions_count:
                if topic_snapshot_key_count == summary.snapshot_count:
                    failures.append(f'Kafka topic contained {summary.snapshot_key_order_regressions_count} regressions '
                                    f'in snapshot key ordering.')
                else:
                    warnings.append(f'Kafka topic contained {summary.snapshot_key_order_regressions_count} regressions '
                                    f'in snapshot key ordering. (This may be okay if more than one snapshot has been '
                                    f'taken.)')
            if summary.unknown_operation_count:
                failures.append(f'Topic contained {summary.unknown_operation_count} messages with an unknown operation '
                                f'type.')
            if summary.tombstone_count and summary.tombstone_count < summary.all_deletes_in_topic:
                failures.append(f'Tombstone record count in topic ({summary.tombstone_count}) is less than the number '
                                f'of deletes ({summary.all_deletes_in_topic}).')
            if summary.delete_count != db_delete_rows:
                failures.append(f'Found {db_delete_rows} delete entries in DB change table but {summary.delete_count} '
                                f'in Kafka topic')
            if summary.insert_count != db_insert_rows:
                failures.append(f'Found {db_insert_rows} insert entries in DB change table but {summary.insert_count} '
                                f'in Kafka topic')
            if summary.update_count != db_update_rows:
                failures.append(f'Found {db_update_rows} update entries in DB change table but {summary.update_count} '
                                f'in Kafka topic')

            print(f'\nSummary for table {source_topic_name} in single-table topic {table.topic_name}:')
            for info in infos:
                print(f'    INFO: {info} ({source_topic_name})')
            if not (warnings or failures):
                print(f'    OK: No problems! ({source_topic_name})')
            else:
                for warning in warnings:
                    print(f'    WARN: {warning} ({source_topic_name})')
                for failure in failures:
                    print(f'    FAIL: {failure} ({source_topic_name})')

            if failures:
                db_data = json.dumps({
                    'db_source_rows': db_source_row_counts,
                    'db_delete_rows': db_delete_rows,
                    'db_insert_rows': db_insert_rows,
                    'db_update_rows': db_update_rows
                })
                logger.debug('Messages summary: %s, DB data: %s, snap progress: %s, change progress: %s', summary,
                             db_data, snap_progress, changes_progress_index)

        for ut_topic, ut_result in summaries_by_unified_topic.items():
            print(f"\nResults from analyzing {ut_result['total_messages_read']} messages from unified "
                  f"topic {ut_topic}:")
            warnings = ut_result['warnings']
            failures = ut_result['failures']

            for source_topic_name, table_summary in ut_result['table_summaries'].items():
                if table_summary.latest_change_seen is None:
                    warnings.append(f'For source topic {source_topic_name}: No change entries found!')
                elif (helpers.naive_utcnow() - table_summary.latest_change_seen) > datetime.timedelta(days=1):
                    warnings.append(f'For source topic {source_topic_name}: Last change entry seen in Kafka was dated '
                                    f'{table_summary.latest_change_seen}.')
                if table_summary.change_index_order_regressions_count:
                    failures.append(
                        f'For source topic {source_topic_name}: Kafka topic contained '
                        f'{table_summary.change_index_order_regressions_count} regressions in change index ordering.')
                if table_summary.unknown_operation_count:
                    failures.append(
                        f'For source topic {source_topic_name}: Topic contained {table_summary.unknown_operation_count} '
                        f'messages with an unknown operation type.')
                if table_summary.snapshot_count:
                    failures.append(
                        f'For source topic {source_topic_name}: Topic contained {table_summary.snapshot_count} '
                        f'unexpected snapshot records.')

            for warning in warnings:
                print(f'    WARN: {warning}')
            for failure in failures:
                print(f'    FAIL: {failure}')

    def _process_single_table_topic(self, table: 'tracked_tables.TrackedTable',
                                    captured_watermarks: List[Tuple[int, int]]) -> TableMessagesSummary:
        table_summary = TableMessagesSummary(table)
        msg_count = 0
        for msg in self._kafka_client.consume_bounded(
                table.topic_name, constants.VALIDATION_MAXIMUM_SAMPLE_SIZE_PER_TOPIC, captured_watermarks):
            msg_count += 1
            deser_msg = self._serializer.deserialize(msg)
            table_summary.process_message(deser_msg)
        logger.info('Validation: consumed %s records from topic %s', msg_count, table.topic_name)
        return table_summary

    def _process_unified_topic(self, topic_name: str, expected_tables: Iterable['tracked_tables.TrackedTable'],
                               captured_watermarks: List[Tuple[int, int]]) -> Dict[str, Any]:
        logger.info('Validation: consuming records from unified topic %s', topic_name)

        table_summaries: Dict[str, TableMessagesSummary] = {t.topic_name: TableMessagesSummary(t) for t in expected_tables}
        warnings: List[str] = []
        failures: List[str] = []
        sample_regression_indices: List[str] = []
        unexpected_table_msg_counts: Dict[str, int] = collections.defaultdict(int)
        total_messages_read: int = 0
        lsn_regressions_count: int = 0
        tombstones_count: int = 0
        prior_read_change_index: Optional[change_index.ChangeIndex] = None
        prior_read_partition: int = 0
        prior_read_offset: int = 0

        for msg in self._kafka_client.consume_bounded(
                topic_name, constants.VALIDATION_MAXIMUM_SAMPLE_SIZE_PER_TOPIC, captured_watermarks):
            total_messages_read += 1
            deser_msg = self._serializer.deserialize(msg)

            # noinspection PyArgumentList
            if not msg.value():
                tombstones_count += 1
                continue

            # noinspection PyTypeChecker,PyArgumentList
            raw_headers = msg.headers()
            if raw_headers is None:
                raise Exception('Unexpected state: Headers missing from unified topic message')
            msg_table_topic_raw = dict(raw_headers)['cdc_to_kafka_original_topic']
            if type(msg_table_topic_raw) is str:
                msg_table_topic = msg_table_topic_raw
            elif type(msg_table_topic_raw) is bytes:
                msg_table_topic = msg_table_topic_raw.decode('utf-8')
            else:
                raise Exception("Unexpected data type in headers")
            if deser_msg.value_dict is None:
                raise Exception('Unexpected state: Missing value_dict from unified topic message')
            msg_change_index = change_index.ChangeIndex.from_dict(deser_msg.value_dict)

            if prior_read_change_index is not None and prior_read_change_index > msg_change_index:
                if len(sample_regression_indices) < 10:
                    sample_regression_indices.append(
                        f'{prior_read_change_index} @ p{prior_read_partition}:o{prior_read_offset} -> '
                        f'{msg_change_index} @ p{msg.partition()}:o{msg.offset()}')
                    lsn_regressions_count += 1

            prior_read_change_index = msg_change_index
            prior_read_partition = msg.partition() or prior_read_partition
            prior_read_offset = msg.offset() or prior_read_offset

            if msg_table_topic in table_summaries:
                table_summaries[msg_table_topic].process_message(deser_msg)
            else:
                unexpected_table_msg_counts[msg_table_topic] += 1

        if lsn_regressions_count:
            failures.append(f'{lsn_regressions_count} LSN ordering regressions encountered, with examples '
                            f'including {sample_regression_indices}.')

        if tombstones_count:
            failures.append(f'{tombstones_count} unexpected deletion tombstones encountered')

        if unexpected_table_msg_counts:
            warnings.append(f'Topic contained messages from unanticipated source topics: '
                            f'{json.dumps(unexpected_table_msg_counts)}')

        return {
            'total_messages_read': total_messages_read,
            'table_summaries': table_summaries,
            'warnings': warnings,
            'failures': failures
        }
