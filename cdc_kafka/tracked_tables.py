import hashlib
import itertools
import logging
import uuid
from typing import Tuple, Dict, List, Any, Callable, Optional, Generator, TYPE_CHECKING

import bitarray
import pyodbc

from . import avro_from_sql, constants, change_index, options, sql_queries, sql_query_subprocess, parsed_row, helpers

if TYPE_CHECKING:
    from .metric_reporting import accumulator

logger = logging.getLogger(__name__)

NEW_SNAPSHOT = object()


class TrackedField(object):
    def __init__(self, name: str, sql_type_name: str, change_table_ordinal: int, primary_key_ordinal: int,
                 decimal_precision: int, decimal_scale: int, force_avro_long: bool,
                 truncate_after: Optional[int] = None) -> None:
        self.name: str = name
        self.sql_type_name: str = sql_type_name
        self.change_table_ordinal: int = change_table_ordinal
        self.primary_key_ordinal: int = primary_key_ordinal
        self.avro_schema: Dict[str, Any] = avro_from_sql.avro_schema_from_sql_type(
            name, sql_type_name, decimal_precision, decimal_scale, False, force_avro_long)
        self.nullable_avro_schema: Dict[str, Any] = avro_from_sql.avro_schema_from_sql_type(
            name, sql_type_name, decimal_precision, decimal_scale, True, force_avro_long)
        self.transform_fn: Optional[Callable[[Any], Any]] = avro_from_sql.avro_transform_fn_from_sql_type(sql_type_name)

        if truncate_after is not None:
            if self.sql_type_name not in avro_from_sql.SQL_STRING_TYPES:
                raise Exception(f'A truncation length was specified for field {name} but it does not appear to be a '
                                f'string field (SQL type is {sql_type_name}).')
            orig_transform = self.transform_fn
            if orig_transform is not None:
                # TODO: this prevents orig_transform from ever receiving a None argument; is that okay??
                self.transform_fn = lambda x: orig_transform(x)[:int(truncate_after)] if x is not None else x
            else:
                self.transform_fn = lambda x: x[:int(truncate_after)] if x is not None else x


class TrackedTable(object):
    def __init__(self, db_conn: pyodbc.Connection, metrics_accumulator: 'accumulator.Accumulator',
                 sql_query_processor: sql_query_subprocess.SQLQueryProcessor, schema_name: str, table_name: str,
                 capture_instance_name: str, topic_name: str, min_lsn: bytes, snapshot_allowed: bool,
                 db_row_batch_size: int) -> None:
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

        # Most of the below properties are not set until sometime after `finalize_table` is called:

        self.key_schema_id: int = -1
        self.value_schema_id: int = -1
        self.key_schema: Dict[str, Any] = {}
        self.value_schema: Dict[str, Any] = {}
        self.key_fields: Tuple[TrackedField] = tuple()
        self.value_fields: Tuple[TrackedField] = tuple()
        self.max_polled_change_index: change_index.ChangeIndex = change_index.LOWEST_CHANGE_INDEX
        self.change_reads_are_lagging: bool = True
        self.snapshot_complete: bool = False
        self.min_lsn: bytes = min_lsn

        self._key_field_names: Tuple[str] = tuple()
        self._key_field_source_table_ordinals: Tuple[int] = tuple()
        self._value_field_names: List[str] = []
        self._last_read_key_for_snapshot: Optional[Tuple] = None
        self._odbc_columns: Tuple[pyodbc.Row, ...] = tuple()
        self._change_rows_query: Optional[str] = None
        self._change_rows_query_param_types: List[Tuple[int, int, int]] = []
        self._snapshot_rows_query: Optional[str] = None
        self._snapshot_rows_query_param_types: List[Tuple[int, int, int]] = []
        self._initial_snapshot_rows_query: Optional[str] = None
        self._fields_added_pending_finalization: List[TrackedField] = []
        self._finalized: bool = False
        self._has_pk: bool = False
        self._snapshot_query_pending: bool = False
        self._changes_query_pending: bool = False
        self._changes_query_queue_name: str = self.fq_name + constants.CHANGE_ROWS_KIND
        self._snapshot_query_queue_name: str = self.fq_name + constants.SNAPSHOT_ROWS_KIND

    @property
    def last_read_key_for_snapshot_display(self) -> Optional[str]:
        if not self._last_read_key_for_snapshot or self._last_read_key_for_snapshot == NEW_SNAPSHOT:
            return None
        return ', '.join([f'{k}: {v}' for k, v in zip(self._key_field_names, self._last_read_key_for_snapshot)])

    def append_field(self, field: TrackedField) -> None:
        self._fields_added_pending_finalization.append(field)

    def get_source_table_count(self, low_key: Tuple, high_key: Tuple) -> int:
        with self._db_conn.cursor() as cursor:
            q, p = sql_queries.get_table_count(self.schema_name, self.table_name, self._key_field_names,
                                               self._odbc_columns)
            cursor.setinputsizes(p)
            cursor.execute(q, low_key + high_key)
            return cursor.fetchval()

    def get_change_table_counts(self, highest_change_index: change_index.ChangeIndex) -> Tuple[int, int, int]:
        with self._db_conn.cursor() as cursor:
            deletes, inserts, updates = 0, 0, 0
            q, p = sql_queries.get_change_table_count_by_operation(
                helpers.quote_name(helpers.get_fq_change_table_name(self.capture_instance_name)))
            cursor.setinputsizes(p)
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
        start_from_key_for_snapshot: Optional[Dict[str, Any]], lsn_gap_handling: str,
        schema_id_getter: Callable[[str, Dict[str, Any], Dict[str, Any]], Tuple[int, int]] = None,
        progress_reset_fn: Callable[[str, str], None] = None,
        record_snapshot_completion_fn: Callable[[str], None] = None
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
            elif lsn_gap_handling == options.LSN_GAP_HANDLING_IGNORE:
                logger.warning('%s Proceeding anyway since lsn_gap_handling is set to "%s"!',
                               msg, options.LSN_GAP_HANDLING_IGNORE)
            elif lsn_gap_handling == options.LSN_GAP_HANDLING_BEGIN_NEW_SNAPSHOT:
                if self.snapshot_allowed:
                    start_from_key_for_snapshot = None
                    logger.warning('%s Beginning new table snapshot!', msg)
                    progress_reset_fn(self.topic_name, constants.ALL_PROGRESS_KINDS)
                else:
                    raise Exception(
                        f'{msg} lsn_gap_handling was set to "{options.LSN_GAP_HANDLING_BEGIN_NEW_SNAPSHOT}", but due '
                        f'to white/black-listing, snapshotting of table {self.fq_name} is not allowed!')
            else:
                raise Exception(msg + f' Cannot continue! Parameter lsn_gap_handling was set to "{lsn_gap_handling}".')

        self.max_polled_change_index = start_after_change_table_index
        self.value_fields = tuple(sorted(self._fields_added_pending_finalization, key=lambda f: f.change_table_ordinal))
        self._value_field_names = [f.name for f in self.value_fields]
        self._fields_added_pending_finalization = []

        key_fields = [f for f in self.value_fields if f.primary_key_ordinal is not None]
        if not key_fields:
            self._has_pk = False
            key_fields = [TrackedField(
                constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT, 'varchar', 0, 0, 0, 0, False)]
        else:
            self._has_pk = True

        self.key_fields = tuple(sorted(key_fields, key=lambda f: f.primary_key_ordinal))
        self._key_field_names = tuple([kf.name for kf in self.key_fields])
        self._key_field_source_table_ordinals = tuple([kf.change_table_ordinal for kf in self.key_fields])
        key_schema_fields = [kf.avro_schema for kf in self.key_fields]

        self.key_schema = {
            "name": f"{self.schema_name}_{self.table_name}_cdc__key",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": key_schema_fields
        }

        value_fields_plus_metadata_fields = avro_from_sql.get_cdc_metadata_fields_avro_schemas(
            self.fq_name, self._value_field_names) + [f.nullable_avro_schema for f in self.value_fields]

        self.value_schema = {
            "name": f"{self.schema_name}_{self.table_name}_cdc__value",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": value_fields_plus_metadata_fields
        }

        if schema_id_getter:
            self.key_schema_id, self.value_schema_id = schema_id_getter(self.topic_name, self.key_schema,
                                                                        self.value_schema)

        with self._db_conn.cursor() as cursor:
            q, p = sql_queries.get_change_table_index_cols()
            cursor.setinputsizes(p)
            cursor.execute(q, helpers.get_fq_change_table_name(self.capture_instance_name))
            change_table_clustered_idx_cols = [r[0] for r in cursor.fetchall()]

            self._odbc_columns = tuple(cursor.columns(schema=self.schema_name, table=self.table_name).fetchall())

        required_metadata_cols_ordered = [constants.DB_LSN_COL_NAME, constants.DB_SEQVAL_COL_NAME,
                                          constants.DB_OPERATION_COL_NAME]
        found_metadata_cols = [c for c in change_table_clustered_idx_cols if c in required_metadata_cols_ordered]

        if found_metadata_cols != required_metadata_cols_ordered:
            raise Exception(f'The index for change table {helpers.get_fq_change_table_name(self.capture_instance_name)} '
                            f'did not contain the expected CDC metadata columns, or contained them in the wrong order. '
                            f'Index columns found were: {change_table_clustered_idx_cols}')

        self._change_rows_query, self._change_rows_query_param_types = sql_queries.get_change_rows(
            self.db_row_batch_size, helpers.quote_name(helpers.get_fq_change_table_name(self.capture_instance_name)),
            self._value_field_names, change_table_clustered_idx_cols)

        if not self.snapshot_allowed:
            self.snapshot_complete = True
        else:
            columns_actually_on_base_table = {x[3] for x in self._odbc_columns}
            columns_no_longer_on_base_table = set(self._value_field_names) - columns_actually_on_base_table

            if columns_no_longer_on_base_table:
                logger.warning('Some column(s) found in the capture instance appear to no longer be present on base '
                               'table %s. Column(s): %s',
                               self.fq_name,
                               ', '.join(columns_no_longer_on_base_table))
            if self._has_pk:
                self._snapshot_rows_query, self._snapshot_rows_query_param_types = sql_queries.get_snapshot_rows(
                    self.db_row_batch_size, self.schema_name, self.table_name, self._value_field_names,
                    columns_no_longer_on_base_table, self._key_field_names, False, self._odbc_columns)

                if start_from_key_for_snapshot == constants.SNAPSHOT_COMPLETION_SENTINEL:
                    self.snapshot_complete = True
                elif start_from_key_for_snapshot:
                    key_min_tuple = tuple(self._get_min_key_value() or [])
                    start_key_tuple = tuple([start_from_key_for_snapshot[kfn] for kfn in self._key_field_names])

                    if key_min_tuple and key_min_tuple == start_key_tuple:
                        self.snapshot_complete = True
                        if record_snapshot_completion_fn is not None:
                            record_snapshot_completion_fn(self.topic_name)
                    else:
                        self._last_read_key_for_snapshot = start_key_tuple
                else:
                    key_max = self._get_max_key_value()
                    if key_max:
                        logger.info('Table %s is starting a full snapshot, working back from max key (%s)',
                                    self.fq_name,
                                    ', '.join([f'{k}: {v}' for k, v in zip(self._key_field_names, key_max)]))

                        self._initial_snapshot_rows_query, _ = sql_queries.get_snapshot_rows(
                            self.db_row_batch_size, self.schema_name, self.table_name, self._value_field_names,
                            columns_no_longer_on_base_table, self._key_field_names, True, self._odbc_columns)

                        self._last_read_key_for_snapshot = NEW_SNAPSHOT
                    else:
                        logger.warning('Snapshot was requested for table %s but it appears empty.', self.fq_name)
                        self.snapshot_complete = True
                        if record_snapshot_completion_fn is not None:
                            record_snapshot_completion_fn(self.topic_name)
            else:
                raise Exception(
                    f"Snapshotting was requested for table {self.fq_name}, but it does not appear to have a primary "
                    f"key (which is required for snapshotting at this time). You can get past this error by adding "
                    f"the table to the snapshot blacklist")

    def enqueue_snapshot_query(self) -> None:
        if self._snapshot_query_pending:
            raise Exception('enqueue_snapshot_query called while a query was already pending')

        if self.snapshot_allowed and not self.snapshot_complete:
            if self._last_read_key_for_snapshot == NEW_SNAPSHOT:
                snap_query = sql_query_subprocess.SQLQueryRequest(self._snapshot_query_queue_name, None,
                                                                  self._initial_snapshot_rows_query, None, None)
            else:
                snap_query = sql_query_subprocess.SQLQueryRequest(self._snapshot_query_queue_name, None,
                                                                  self._snapshot_rows_query,
                                                                  self._snapshot_rows_query_param_types,
                                                                  self._last_read_key_for_snapshot)
            self._sql_query_processor.enqueue_query(snap_query)
            self._snapshot_query_pending = True

    def enqueue_changes_query(self, lsn_limit: bytes) -> None:
        if self._changes_query_pending:
            raise Exception('enqueue_changes_query called while a query was already pending')

        params = (self.max_polled_change_index.lsn, self.max_polled_change_index.seqval, lsn_limit)
        changes_query = sql_query_subprocess.SQLQueryRequest(
            self._changes_query_queue_name, lsn_limit, self._change_rows_query,
            self._change_rows_query_param_types, params)
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
                    last_row_read = self._parse_db_row(row)
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
                else:
                    logger.info("SNAPSHOT COMPLETED for table %s. Last read key: (%s)", self.fq_name, ', '.join(
                        [f'{k}: {v}' for k, v in zip(self._key_field_names, self._last_read_key_for_snapshot)]))
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
                    last_row_read = self._parse_db_row(row)
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
            q, p = sql_queries.get_change_rows_per_second(
                helpers.quote_name(helpers.get_fq_change_table_name(self.capture_instance_name)))
            cursor.execute(q)
            return cursor.fetchval() or 0

    def _parse_db_row(self, db_row: pyodbc.Row) -> parsed_row.ParsedRow:
        operation_id, event_db_time, lsn, seqval, update_mask, *table_cols = db_row
        operation_name = constants.CDC_OPERATION_ID_TO_NAME[operation_id]

        value_dict = {fld.name: fld.transform_fn(table_cols[ix]) if fld.transform_fn else table_cols[ix]
                      for ix, fld in enumerate(self.value_fields)}

        if operation_id == constants.SNAPSHOT_OPERATION_ID:
            row_kind = constants.SNAPSHOT_ROWS_KIND
            change_idx = None
            value_dict[constants.OPERATION_NAME] = constants.SNAPSHOT_OPERATION_NAME
            value_dict[constants.LSN_NAME] = None
            value_dict[constants.SEQVAL_NAME] = None
        else:
            row_kind = constants.CHANGE_ROWS_KIND
            change_idx = change_index.ChangeIndex(lsn, seqval, operation_id)
            value_dict.update(change_idx.to_avro_ready_dict())

        if operation_id in (constants.PRE_UPDATE_OPERATION_ID, constants.POST_UPDATE_OPERATION_ID):
            value_dict[constants.UPDATED_FIELDS_NAME] = self._updated_col_names_from_mask(update_mask)
        else:
            value_dict[constants.UPDATED_FIELDS_NAME] = self._value_field_names

        value_dict[constants.EVENT_TIME_NAME] = event_db_time.isoformat()

        if self._has_pk:
            ordered_key_field_values: List[Any] = [table_cols[kfo - 1] for kfo in self._key_field_source_table_ordinals]
        else:
            # CAUTION: this strategy for handling PK-less tables means that if columns are added or removed from the
            # capture instance in the future, the key value computed for the same source table row will change:
            m = hashlib.md5()
            m.update(str(zip(self._value_field_names, table_cols)).encode('utf8'))
            row_hash = str(uuid.uuid5(uuid.UUID(bytes=m.digest()), self.fq_name))
            ordered_key_field_values: List[Any] = [row_hash]

        key_dict: Dict[str, Any] = dict(zip(self._key_field_names, ordered_key_field_values))

        return parsed_row.ParsedRow(self.fq_name, row_kind, operation_name, event_db_time, change_idx,
                                    tuple(ordered_key_field_values), self.topic_name, self.key_schema_id,
                                    self.value_schema_id, key_dict, value_dict)

    def _updated_col_names_from_mask(self, cdc_update_mask: bytes) -> List[str]:
        arr = bitarray.bitarray()
        arr.frombytes(cdc_update_mask)
        arr.reverse()
        return list(itertools.compress(self._value_field_names, arr))

    def _get_max_key_value(self) -> Optional[Tuple[Any]]:
        with self._db_conn.cursor() as cursor:
            q, p = sql_queries.get_max_key_value(self.schema_name, self.table_name, self._key_field_names)
            cursor.execute(q)
            return cursor.fetchone()

    def _get_min_key_value(self) -> Optional[Tuple[Any]]:
        with self._db_conn.cursor() as cursor:
            q, p = sql_queries.get_min_key_value(self.schema_name, self.table_name, self._key_field_names)
            cursor.execute(q)
            return cursor.fetchone()
