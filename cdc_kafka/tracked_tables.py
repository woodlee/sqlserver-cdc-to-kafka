import hashlib
import logging
import uuid
from typing import Tuple, List, Any, Optional, Generator, TYPE_CHECKING, Mapping, Sequence, Dict

import pyodbc

from . import constants, change_index, options, sql_queries, sql_query_subprocess, parsed_row, \
    helpers, progress_tracking

if TYPE_CHECKING:
    from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


class TrackedField(object):
    __slots__ = 'name', 'sql_type_name', 'change_table_ordinal', 'primary_key_ordinal', 'decimal_precision', \
        'decimal_scale', 'truncate_after'

    def __init__(self, name: str, sql_type_name: str, change_table_ordinal: int, primary_key_ordinal: int,
                 decimal_precision: int, decimal_scale: int, truncate_after: int = 0) -> None:
        self.name: str = name
        self.sql_type_name: str = sql_type_name
        self.change_table_ordinal: int = change_table_ordinal
        self.primary_key_ordinal: int = primary_key_ordinal
        self.decimal_precision: int = decimal_precision
        self.decimal_scale: int = decimal_scale

        if truncate_after:
            if self.sql_type_name not in constants.SQL_STRING_TYPES:
                raise Exception(f'A truncation length was specified for field {name} but it does not appear to be a '
                                f'string field (SQL type is {sql_type_name}).')
        self.truncate_after: int = truncate_after


class TrackedTable(object):
    def __init__(self, db_conn: pyodbc.Connection, metrics_accumulator: 'accumulator.Accumulator',
                 sql_query_processor: sql_query_subprocess.SQLQueryProcessor, schema_name: str, table_name: str,
                 capture_instance_name: str, topic_name: str, min_lsn: bytes, snapshot_allowed: bool,
                 db_row_batch_size: int, progress_tracker: 'progress_tracking.ProgressTracker') -> None:
        self._db_conn: pyodbc.Connection = db_conn
        self._metrics_accumulator: 'accumulator.Accumulator' = metrics_accumulator
        self._sql_query_processor: sql_query_subprocess.SQLQueryProcessor = sql_query_processor
        self.schema_name: str = schema_name
        self.table_name: str = table_name
        self.capture_instance_name: str = capture_instance_name
        self.topic_name: str = topic_name
        self.snapshot_allowed: bool = snapshot_allowed
        self.fq_name: str = f'{schema_name}.{table_name}'
        self.db_row_batch_size: int = db_row_batch_size
        self.progress_tracker: progress_tracking.ProgressTracker = progress_tracker
        self.truncate_indexes: Dict[int, int] = {}

        # Most of the below properties are not set until sometime after `finalize_table` is called:

        self.key_fields: Tuple[TrackedField, ...] = tuple()
        self.value_fields: Tuple[TrackedField, ...] = tuple()
        self.key_field_names: Tuple[str, ...] = tuple()
        self.value_field_names: List[str] = []
        self.key_field_source_table_ordinals: Tuple[int, ...] = tuple()
        self.max_polled_change_index: change_index.ChangeIndex = change_index.LOWEST_CHANGE_INDEX
        self.change_reads_are_lagging: bool = False
        self.snapshot_complete: bool = False
        self.min_lsn: bytes = min_lsn

        self._last_read_key_for_snapshot: Optional[Sequence[Any]] = None
        self._odbc_columns: Tuple[pyodbc.Row, ...] = tuple()
        self._change_rows_query: str = ''
        self._change_rows_query_param_types: List[Tuple[int, int, Optional[int]]] = []
        self._snapshot_rows_query: str = ''
        self._snapshot_rows_query_param_types: List[Tuple[int, int, Optional[int]]] = []
        self._initial_snapshot_rows_query: str = ''
        self._fields_added_pending_finalization: List[TrackedField] = []
        self._finalized: bool = False
        self._has_pk: bool = False
        self._snapshot_query_pending: bool = False
        self._changes_query_pending: bool = False
        self._changes_query_queue_name: str = self.fq_name + constants.CHANGE_ROWS_KIND
        self._snapshot_query_queue_name: str = self.fq_name + constants.SNAPSHOT_ROWS_KIND

        self.progress_tracker.register_table(self)

    @property
    def last_read_key_for_snapshot_display(self) -> Optional[str]:
        if not self._last_read_key_for_snapshot:
            return None
        return ', '.join([f'{k}: {v}' for k, v in zip(self.key_field_names, self._last_read_key_for_snapshot)])

    def append_field(self, field: TrackedField) -> None:
        field_ix: int = len(self._fields_added_pending_finalization)
        self._fields_added_pending_finalization.append(field)
        if field.truncate_after:
            self.truncate_indexes[field_ix] = field.truncate_after

    def get_source_table_count(self, low_key: Tuple[Any, ...], high_key: Tuple[Any, ...]) -> int:
        with self._db_conn.cursor() as cursor:
            q, p = sql_queries.get_table_count(self.schema_name, self.table_name, self.key_field_names,
                                               self._odbc_columns)
            cursor.setinputsizes(p)  # type: ignore[arg-type]
            cursor.execute(q, low_key + high_key)
            res: int = cursor.fetchval()
            return res

    def get_change_table_counts(self, highest_change_index: change_index.ChangeIndex) -> Tuple[int, int, int]:
        with self._db_conn.cursor() as cursor:
            deletes, inserts, updates = 0, 0, 0
            q, p = sql_queries.get_change_table_count_by_operation(
                helpers.quote_name(helpers.get_fq_change_table_name(self.capture_instance_name)))
            cursor.setinputsizes(p)  # type: ignore[arg-type]
            cursor.execute(q, (highest_change_index.lsn, highest_change_index.seqval, highest_change_index.operation))
            for row in cursor.fetchall():
                if row[1] == 1:
                    deletes = row[0]
                elif row[1] == 2:
                    inserts = row[0]
                elif row[1] == 4:
                    updates = row[0]
                else:
                    raise Exception('Unexpected __$operation')

            return deletes, inserts, updates

    # 'Finalizing' mostly means doing the things we can't do until we know all the table's fields have been added
    def finalize_table(
        self, start_after_change_table_index: change_index.ChangeIndex,
        prior_change_table_max_index: Optional[change_index.ChangeIndex],
        start_from_key_for_snapshot: Optional[Mapping[str, Any]], lsn_gap_handling: str,
        allow_progress_writes: bool = False
    ) -> None:
        if self._finalized:
            raise Exception(f"Attempted to finalize table {self.fq_name} more than once")

        self._finalized = True

        if change_index.LOWEST_CHANGE_INDEX.lsn < start_after_change_table_index.lsn < self.min_lsn:
            msg = (f'The earliest change LSN available in the DB for capture instance {self.capture_instance_name} '
                   f'(0x{self.min_lsn.hex()}) is later than the log position we need to start from based on the prior '
                   f'progress LSN stored in Kafka (0x{start_after_change_table_index.lsn.hex()}).')

            if prior_change_table_max_index and prior_change_table_max_index <= start_after_change_table_index:
                logger.info('%s Proceeding anyway, because it appears that no new entries are present in the prior '
                            'capture instance with an LSN higher than the last changes sent to Kafka.', msg)
                self.progress_tracker.record_changes_progress(self.topic_name, change_index.ChangeIndex(
                    self.min_lsn, change_index.LOWEST_CHANGE_INDEX.seqval, change_index.LOWEST_CHANGE_INDEX.operation))
            elif lsn_gap_handling == options.LSN_GAP_HANDLING_IGNORE:
                logger.warning('%s Proceeding anyway since lsn_gap_handling is set to "%s"!',
                               msg, options.LSN_GAP_HANDLING_IGNORE)
            elif lsn_gap_handling == options.LSN_GAP_HANDLING_BEGIN_NEW_SNAPSHOT:
                if self.snapshot_allowed and allow_progress_writes:
                    self.progress_tracker.reset_progress(self.topic_name, constants.ALL_PROGRESS_KINDS, self.fq_name,
                                                         True, start_from_key_for_snapshot)
                    start_from_key_for_snapshot = None
                    logger.warning('%s Beginning new table snapshot!', msg)
                else:
                    raise Exception(
                        f'{msg} lsn_gap_handling was set to "{options.LSN_GAP_HANDLING_BEGIN_NEW_SNAPSHOT}", but due '
                        f'to configured inclusion/exclusion regexes, snapshotting of table {self.fq_name} is not '
                        f'allowed!')
            else:
                raise Exception(msg + f' Cannot continue! Parameter lsn_gap_handling was set to "{lsn_gap_handling}".')

        self.max_polled_change_index = start_after_change_table_index
        self.value_fields = tuple(sorted(self._fields_added_pending_finalization, key=lambda f: f.change_table_ordinal))
        self.value_field_names = [f.name for f in self.value_fields]
        self._fields_added_pending_finalization = []

        key_fields = [f for f in self.value_fields if f.primary_key_ordinal is not None]
        if not key_fields:
            self._has_pk = False
            key_fields = [TrackedField(
                constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT, 'varchar', 0, 0, 0, 0, False)]
        else:
            self._has_pk = True

        self.key_fields = tuple(sorted(key_fields, key=lambda f: f.primary_key_ordinal))
        self.key_field_names = tuple([kf.name for kf in self.key_fields])
        self.key_field_source_table_ordinals = tuple([kf.change_table_ordinal for kf in self.key_fields])
        capture_instance_name = helpers.get_fq_change_table_name(self.capture_instance_name)

        with self._db_conn.cursor() as cursor:
            q, p = sql_queries.get_change_table_index_cols()
            cursor.setinputsizes(p)  # type: ignore[arg-type]
            cursor.execute(q, capture_instance_name)
            change_table_clustered_idx_cols = [r[0] for r in cursor.fetchall()]

            self._odbc_columns = tuple(cursor.columns(schema=self.schema_name, table=self.table_name).fetchall())

        required_metadata_cols_ordered = [constants.DB_LSN_COL_NAME, constants.DB_SEQVAL_COL_NAME,
                                          constants.DB_OPERATION_COL_NAME]
        found_metadata_cols = [c for c in change_table_clustered_idx_cols if c in required_metadata_cols_ordered]

        if found_metadata_cols != required_metadata_cols_ordered:
            raise Exception(f'The index for change table {capture_instance_name} did not contain the expected CDC '
                            f'metadata columns, or contained them in the wrong order. Index columns found '
                            f'were: {change_table_clustered_idx_cols}')

        self._change_rows_query, self._change_rows_query_param_types = sql_queries.get_change_rows(
            self.db_row_batch_size, helpers.quote_name(capture_instance_name), self.value_field_names,
            change_table_clustered_idx_cols)

        if not self.snapshot_allowed:
            self.snapshot_complete = True
        else:
            columns_actually_on_base_table = {x[3] for x in self._odbc_columns}
            columns_no_longer_on_base_table = set(self.value_field_names) - columns_actually_on_base_table

            if columns_no_longer_on_base_table:
                logger.warning('Some column(s) found in the capture instance appear to no longer be present on base '
                               'table %s. Column(s): %s',
                               self.fq_name,
                               ', '.join(columns_no_longer_on_base_table))
            if self._has_pk:
                self._snapshot_rows_query, self._snapshot_rows_query_param_types = sql_queries.get_snapshot_rows(
                    self.db_row_batch_size, self.schema_name, self.table_name, self.value_field_names,
                    columns_no_longer_on_base_table, self.key_field_names, False, self._odbc_columns)

                if start_from_key_for_snapshot == constants.SNAPSHOT_COMPLETION_SENTINEL:
                    self.snapshot_complete = True
                elif start_from_key_for_snapshot:
                    key_min_tuple = tuple(self._get_min_key_value() or [])
                    start_key_tuple = tuple([start_from_key_for_snapshot[kfn] for kfn in self.key_field_names])

                    if key_min_tuple and key_min_tuple == start_key_tuple:
                        self.snapshot_complete = True
                        if allow_progress_writes:
                            self.progress_tracker.record_snapshot_progress(self.topic_name,
                                                                           constants.SNAPSHOT_COMPLETION_SENTINEL)
                    else:
                        if allow_progress_writes:
                            start_key_dict = dict(zip(self.key_field_names, start_key_tuple))
                            self.progress_tracker.log_snapshot_resumed(self.topic_name, self.fq_name, start_key_dict)

                        self._last_read_key_for_snapshot = start_key_tuple
                else:
                    key_max = self._get_max_key_value()
                    if key_max:
                        key_max_map = dict(zip(self.key_field_names, key_max))
                        logger.info('Table %s is starting a full snapshot, working back from max key %s',
                                    self.fq_name, key_max_map)

                        self._initial_snapshot_rows_query, _ = sql_queries.get_snapshot_rows(
                            self.db_row_batch_size, self.schema_name, self.table_name, self.value_field_names,
                            columns_no_longer_on_base_table, self.key_field_names, True, self._odbc_columns)

                        if allow_progress_writes:
                            self.progress_tracker.log_snapshot_started(self.topic_name, self.fq_name, key_max_map)

                        self._last_read_key_for_snapshot = None
                    else:
                        logger.warning('Snapshot was requested for table %s but it appears empty.', self.fq_name)
                        self.snapshot_complete = True
                        if allow_progress_writes:
                            self.progress_tracker.record_snapshot_progress(self.topic_name,
                                                                           constants.SNAPSHOT_COMPLETION_SENTINEL)
            else:
                raise Exception(
                    f"Snapshotting was requested for table {self.fq_name}, but it does not appear to have a primary "
                    f"key (which is required for snapshotting at this time). You can get past this error by adding "
                    f"the table to the snapshot exclusion regex")

    def enqueue_snapshot_query(self) -> None:
        if self._snapshot_query_pending:
            raise Exception('enqueue_snapshot_query called while a query was already pending')

        if self.snapshot_allowed and not self.snapshot_complete:
            if self._last_read_key_for_snapshot is None:
                snap_query = sql_query_subprocess.SQLQueryRequest(self._snapshot_query_queue_name, None,
                                                                  self._initial_snapshot_rows_query, [], [],
                                                                  self._parse_db_row)
            else:
                snap_query = sql_query_subprocess.SQLQueryRequest(self._snapshot_query_queue_name, None,
                                                                  self._snapshot_rows_query,
                                                                  self._snapshot_rows_query_param_types,
                                                                  self._last_read_key_for_snapshot,
                                                                  self._parse_db_row)
            self._sql_query_processor.enqueue_query(snap_query)
            self._snapshot_query_pending = True

    def enqueue_changes_query(self, lsn_limit: bytes) -> None:
        if self._changes_query_pending:
            raise Exception('enqueue_changes_query called while a query was already pending')

        params = (self.max_polled_change_index.lsn, self.max_polled_change_index.seqval, lsn_limit)
        changes_query = sql_query_subprocess.SQLQueryRequest(
            self._changes_query_queue_name, lsn_limit, self._change_rows_query,
            self._change_rows_query_param_types, params, self._parse_db_row)
        self._sql_query_processor.enqueue_query(changes_query)
        self._changes_query_pending = True

    def retrieve_snapshot_query_results(self) -> Generator[parsed_row.ParsedRow, None, None]:
        if self._snapshot_query_pending:
            res = self._sql_query_processor.get_result(self._snapshot_query_queue_name)
            if res:
                self._snapshot_query_pending = False
                row_ctr = 0
                last_row_read = None
                for row in res.result_rows:
                    last_row_read = row
                    yield last_row_read
                    row_ctr += 1
                self._metrics_accumulator.register_db_query(
                    res.query_took_sec, constants.SNAPSHOT_ROWS_KIND, row_ctr)
                logger.debug('Rows polled - kind: %s\tnbr returned: %s\ttook ms: %s\tsource: %s @ %s',
                             constants.SNAPSHOT_ROWS_KIND.ljust(14),
                             str(row_ctr).rjust(5), str(int(res.query_took_sec * 1000)).rjust(5),
                             self.fq_name, res.query_params)
                if last_row_read is not None:
                    self._last_read_key_for_snapshot = last_row_read.ordered_key_field_values
                if row_ctr < self.db_row_batch_size:
                    last_read: str = '<none>'
                    if self._last_read_key_for_snapshot:
                        last_read = ', '.join([f'{k}: {v}' for k, v in
                                               zip(self.key_field_names, self._last_read_key_for_snapshot)])
                    logger.info("SNAPSHOT COMPLETED for table %s. Last read key: (%s)", self.fq_name, last_read)
                    self._last_read_key_for_snapshot = None
                    self.snapshot_complete = True
            else:
                raise Exception('TrackedTable.retrieve_snapshot_query_results failed retrieving SQL query results.')

    def retrieve_changes_query_results(self) -> Generator[parsed_row.ParsedRow, None, None]:
        if self._changes_query_pending:
            res = self._sql_query_processor.get_result(self._changes_query_queue_name)
            if res:
                self._changes_query_pending = False
                row_ctr = 0
                last_row_read = None
                for row in res.result_rows:
                    last_row_read = row
                    yield last_row_read
                    row_ctr += 1
                self._metrics_accumulator.register_db_query(
                    res.query_took_sec, constants.CHANGE_ROWS_KIND, row_ctr)
                lsn = res.query_params[0].hex()
                logger.debug('Rows polled - kind: %s\tnbr returned: %s\ttook ms: %s\tsource: %s @ %s',
                             constants.CHANGE_ROWS_KIND.ljust(14),
                             str(row_ctr).rjust(5), str(int(res.query_took_sec * 1000)).rjust(5),
                             self.fq_name, f'0x{lsn[:8]} {lsn[8:16]} {lsn[16:]}')
                if row_ctr == self.db_row_batch_size:
                    if not (last_row_read and last_row_read.change_idx):
                        raise Exception('Unexpected state.')
                    self.change_reads_are_lagging = True
                    self.max_polled_change_index = last_row_read.change_idx
                else:
                    self.change_reads_are_lagging = False
                    self.max_polled_change_index = change_index.ChangeIndex(
                        res.reflected_query_request_metadata, change_index.HIGHEST_CHANGE_INDEX.seqval,
                        change_index.HIGHEST_CHANGE_INDEX.operation)
            else:
                raise Exception('TrackedTable.retrieve_changes_query_results failed retrieving SQL query results.')

    def get_change_rows_per_second(self) -> int:
        with self._db_conn.cursor() as cursor:
            q, _ = sql_queries.get_change_rows_per_second(
                helpers.quote_name(helpers.get_fq_change_table_name(self.capture_instance_name)))
            cursor.execute(q)
            return cursor.fetchval() or 0

    @staticmethod
    def cut_str_to_bytes(s: str, max_bytes: int) -> Tuple[int, str]:
        # Mostly copied from https://github.com/halloleo/unicut/blob/master/truncate.py
        def safe_b_of_i(encoded: bytes, i: int) -> int:
            try:
                return encoded[i]
            except IndexError:
                return 0

        if s == '' or max_bytes < 1:
            return 0, ''

        b = s[:max_bytes].encode('utf-8')[:max_bytes]

        if b[-1] & 0b10000000:
            last_11x_index = [
                i
                for i in range(-1, -5, -1)
                if safe_b_of_i(b, i) & 0b11000000 == 0b11000000
            ][0]

            last_11x = b[last_11x_index]
            last_char_length = 1
            if not last_11x & 0b00100000:
                last_char_length = 2
            elif not last_11x & 0b0010000:
                last_char_length = 3
            elif not last_11x & 0b0001000:
                last_char_length = 4

            if last_char_length > -last_11x_index:
                # remove the incomplete character
                b = b[:last_11x_index]

        return len(b), b.decode('utf-8')

    def _parse_db_row(self, db_row: pyodbc.Row) -> parsed_row.ParsedRow:
        operation_id, event_db_time, lsn, seqval, update_mask, *table_cols = db_row

        if operation_id == constants.SNAPSHOT_OPERATION_ID:
            change_idx = change_index.LOWEST_CHANGE_INDEX
        else:
            change_idx = change_index.ChangeIndex(lsn, seqval, operation_id)

        extra_headers: Dict[str, str | bytes] = {}

        for ix, max_length in self.truncate_indexes.items():
            # The '* 4' below is because that's the maximum possible byte length of a UTF-8 encoded character--just
            # trying to optimize away the extra code inside the `if` block when possible:
            if table_cols[ix] and len(table_cols[ix]) * 4 > max_length:
                original_encoded_length = len(table_cols[ix].encode('utf-8'))
                if original_encoded_length > max_length:
                    new_encoded_length, table_cols[ix] = TrackedTable.cut_str_to_bytes(table_cols[ix], max_length)
                    extra_headers[f'cdc_to_kafka_truncated_field__{self.value_field_names[ix]}'] = \
                        f'{original_encoded_length},{new_encoded_length}'

        ordered_key_field_values: List[Any]
        if self._has_pk:
            ordered_key_field_values = [table_cols[kfo - 1] for kfo in self.key_field_source_table_ordinals]
        else:
            # CAUTION: this strategy for handling PK-less tables means that if columns are added or removed from the
            # capture instance in the future, the key value computed for the same source table row will change:
            m = hashlib.md5()
            m.update(str(zip(self.value_field_names, table_cols)).encode('utf8'))
            row_hash = str(uuid.uuid5(uuid.UUID(bytes=m.digest()), self.fq_name))
            ordered_key_field_values = [row_hash]

        return parsed_row.ParsedRow(self.topic_name, operation_id, update_mask, event_db_time, change_idx,
                                    tuple(ordered_key_field_values), table_cols, extra_headers)

    def _get_max_key_value(self) -> Optional[Tuple[Any, ...]]:
        with self._db_conn.cursor() as cursor:
            q, _ = sql_queries.get_max_key_value(self.schema_name, self.table_name, self.key_field_names)
            cursor.execute(q)
            row: pyodbc.Row | None = cursor.fetchone()
            if row:
                return tuple(row)
            return None

    def _get_min_key_value(self) -> Optional[Tuple[Any, ...]]:
        with self._db_conn.cursor() as cursor:
            q, _ = sql_queries.get_min_key_value(self.schema_name, self.table_name, self.key_field_names)
            cursor.execute(q)
            row: pyodbc.Row | None = cursor.fetchone()
            if row:
                return tuple(row)
            return None
