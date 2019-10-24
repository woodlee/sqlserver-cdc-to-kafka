import collections
import datetime
import hashlib
import json
import logging
import re
import uuid
from functools import total_ordering
from typing import Tuple, Dict, List, Union, Any, Callable

import avro
import confluent_kafka
import pyodbc

from . import avro_from_sql, constants

logger = logging.getLogger(__name__)

NEW_SNAPSHOT = object()


@total_ordering
class ChangeTableIndex(object):
    # SQL Server CDC capture tables have a single compound index on (lsn, seqval, operation)
    def __init__(self, lsn: bytes, seqval: bytes, operation: int):
        self.lsn: bytes = lsn
        self.seqval: bytes = seqval
        if isinstance(operation, int):
            self.operation: int = operation
        else:
            self.operation: int = constants.CDC_OPERATION_NAME_TO_ID[operation]

    def __eq__(self, other) -> bool:
        return self.lsn + self.seqval + bytes([self.operation]) == \
               other.lsn + other.seqval + bytes([other.operation])

    def __lt__(self, other) -> bool:
        return self.lsn + self.seqval + bytes([self.operation]) < \
               other.lsn + other.seqval + bytes([other.operation])

    def __repr__(self) -> str:
        return f'0x{self.lsn.hex()}:0x{self.seqval.hex()}:{self.operation}'


class TrackedField(object):
    def __init__(self, name: str, sql_type_name: str, change_table_ordinal: int, primary_key_ordinal: int,
                 decimal_precision: int, decimal_scale: int):
        self.name: str = name
        self.sql_type_name: str = sql_type_name
        self.change_table_ordinal: int = change_table_ordinal
        self.primary_key_ordinal: int = primary_key_ordinal
        self.avro_schema: Dict = avro_from_sql.avro_schema_from_sql_type(
            name, sql_type_name, decimal_precision, decimal_scale, False)
        self.nullable_avro_schema: Dict = avro_from_sql.avro_schema_from_sql_type(
            name, sql_type_name, decimal_precision, decimal_scale, True)
        self.transform_fn: Callable = avro_from_sql.avro_transform_fn_from_sql_type(sql_type_name)


class TrackedTable(object):
    _REGISTERED_TABLES_BY_KAFKA_TOPIC: Dict[str, 'TrackedTable'] = {}
    _DB_TIME_DELTA: Union[None, datetime.timedelta] = None
    _DB_TIME_DELTA_LAST_REFRESH: datetime.datetime = datetime.datetime.min

    def __init__(self, db_conn: pyodbc.Connection, schema_name: str, table_name: str, capture_instance_name: str,
                 topic_name: str, min_lsn: bytes, snapshot_allowed: bool):
        self.db_conn: pyodbc.Connection = db_conn
        self.schema_name: str = schema_name
        self.table_name: str = table_name
        self.capture_instance_name: str = capture_instance_name
        self.topic_name: str = topic_name
        self.min_lsn: bytes = min_lsn
        self.snapshot_allowed: bool = snapshot_allowed

        self.fq_name: str = f'{self.schema_name}.{self.table_name}'

        # Most of the below properties are not set until `finalize_table` is called:
        self.snapshot_complete: bool = False
        self.key_schema: Union[None, avro.schema.RecordSchema] = None
        self.value_schema: Union[None, avro.schema.RecordSchema] = None

        self._key_fields: Union[None, List[TrackedField]] = None
        self._key_field_names: Union[None, List[str]] = None
        self._key_field_positions: Union[None, List[int]] = None
        self._quoted_key_field_names: Union[None, List[str]] = None
        self._value_fields: Union[None, List[TrackedField]] = None
        self._value_field_names: Union[None, List[str]] = None
        self._last_read_key_for_snapshot: Union[None, Tuple] = None
        self._change_rows_query: Union[None, str] = None
        self._snapshot_rows_query: Union[None, str] = None
        self._initial_snapshot_rows_query: Union[None, str] = None
        self._last_read_change_table_index: Union[None, ChangeTableIndex] = None

        self._fields_added_pending_finalization: List[TrackedField] = []
        self._row_buffer: collections.deque = collections.deque()
        self._cursor: pyodbc.Cursor = self.db_conn.cursor()
        self._last_db_poll_time: datetime.datetime = constants.BEGINNING_DATETIME
        self._finalized: bool = False
        self._has_pk: bool = False
        self._lagging: bool = False

    @property
    def last_read_key_for_snapshot_display(self):
        if not self._last_read_key_for_snapshot or self._last_read_key_for_snapshot == NEW_SNAPSHOT:
            return None
        return ', '.join([f'{k}: {v}' for k, v in zip(self._key_field_names, self._last_read_key_for_snapshot)])

    @staticmethod  # Needs to be static to be called by the Kafka event produce callback
    def progress_message_extractor(topic_name: str, msg_value: Dict[str, Any]) -> \
            Tuple[Dict[str, Any], avro.schema.RecordSchema, Dict[str, Any], avro.schema.RecordSchema]:

        table = TrackedTable._REGISTERED_TABLES_BY_KAFKA_TOPIC[topic_name]
        key = {"topic_name": topic_name, "capture_instance_name": table.capture_instance_name}

        if msg_value[constants.OPERATION_NAME] == constants.SNAPSHOT_OPERATION_NAME:
            key['progress_kind'] = constants.SNAPSHOT_ROWS_PROGRESS_KIND
            value = {
                'last_published_snapshot_key_field_values': [{
                    'field_name': f.name,
                    'sql_type': f.sql_type_name,
                    'value_as_string': str(msg_value[f.name])
                } for f in table._key_fields]
            }
        else:
            key['progress_kind'] = constants.CHANGE_ROWS_PROGRESS_KIND
            value = {
                'last_published_change_table_lsn': msg_value[constants.LSN_NAME],
                'last_published_change_table_seqval': msg_value[constants.SEQVAL_NAME],
                'last_published_change_table_operation':
                    constants.CDC_OPERATION_NAME_TO_ID[msg_value[constants.OPERATION_NAME]]
            }

        return key, constants.PROGRESS_MESSAGE_AVRO_KEY_SCHEMA, value, constants.PROGRESS_MESSAGE_AVRO_VALUE_SCHEMA

    def pop_next(self) -> Tuple[Tuple, Union[Dict, None], Union[Dict, None]]:
        self._maybe_refresh_buffer()

        if len(self._row_buffer) == 0:
            return self._get_queue_priority_tuple(None), None, None

        next_row = self._row_buffer.popleft()
        msg_key, msg_value = self._message_kv_from_change_row(next_row)

        return self._get_queue_priority_tuple(next_row), msg_key, msg_value

    def add_field(self, field: TrackedField) -> None:
        self._fields_added_pending_finalization.append(field)

    def get_source_table_count(self):
        with self.db_conn.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM [{self.schema_name}].[{self.table_name}]')
            return cursor.fetchval()

    def get_change_table_counts(self):
        with self.db_conn.cursor() as cursor:
            delete, insert, update = 0, 0, 0
            cursor.execute(f'SELECT COUNT(*), __$operation AS op FROM cdc.[{self.capture_instance_name}_CT] '
                           f'WHERE __$operation != 3 GROUP BY __$operation')
            for row in cursor.fetchall():
                if row[1] == 1:
                    delete = row[0]
                elif row[1] == 2:
                    insert = row[0]
                elif row[1] == 4:
                    update = row[0]
                else:
                    raise Exception('Unexpected __$operation')

            return delete, insert, update

    # 'Finalizing' mostly means doing the things we can't do until we know all of the table's fields have been added
    def finalize_table(self, start_after_change_table_index: ChangeTableIndex,
                       start_from_key_for_snapshot: Tuple[Any]) -> None:
        if self._finalized:
            raise Exception(f"Attempted to finalize table {self.fq_name} more than once")

        self._finalized = True

        if start_after_change_table_index.lsn > constants.BEGINNING_CHANGE_TABLE_INDEX.lsn:
            if self.min_lsn > start_after_change_table_index.lsn:
                # TODO - this should maybe trigger a re-snapshot. For now just bail.
                raise Exception(
                    f'The earliest change LSN available in the DB for capture instance {self.capture_instance_name} '
                    f'(0x{self.min_lsn.hex()}) is later than the log position we need to start from based on the prior '
                    f'progress stored in Kafka ((0x{start_after_change_table_index.lsn.hex()}). Cannot continue.')

        self._last_read_change_table_index = start_after_change_table_index

        self._value_fields = sorted(self._fields_added_pending_finalization, key=lambda f: f.change_table_ordinal)
        self._value_field_names = [f.name for f in self._value_fields]

        self._fields_added_pending_finalization = None

        key_fields = [f for f in self._value_fields if f.primary_key_ordinal is not None]
        if not key_fields:
            self._has_pk = False
            key_fields = [TrackedField(
                constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT, 'varchar', 0, 0, 0, 0)]
        else:
            self._has_pk = True

        self._key_fields = sorted(key_fields, key=lambda f: f.primary_key_ordinal)
        self._key_field_names = [f.name for f in self._key_fields]
        self._key_field_positions = [f.change_table_ordinal for f in self._key_fields]
        self._quoted_key_field_names = [f'[{f.name}]' for f in self._key_fields]

        self.key_schema = confluent_kafka.avro.loads(json.dumps({
            "name": f"{self.schema_name}_{self.table_name}_cdc__key",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": [f.avro_schema for f in self._key_fields]
        }))

        value_fields_plus_metadata_fields = avro_from_sql.get_cdc_metadata_fields_avro_schemas(
            self._value_field_names) + [f.nullable_avro_schema for f in self._value_fields]

        self.value_schema = confluent_kafka.avro.loads(json.dumps({
            "name": f"{self.schema_name}_{self.table_name}_cdc__value",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": value_fields_plus_metadata_fields
        }))

        select_column_specs = ', '.join([f'ct.[{f}]' for f in self._value_field_names])

        self._change_rows_query = constants.CHANGE_ROWS_QUERY_TEMPLATE.format(
            fields=select_column_specs, capture_instance_name=self.capture_instance_name)

        if self.snapshot_allowed:
            if self._has_pk:
                # For multi-column primary keys, this builds a WHERE clause of the following form, assuming
                # for example a PK on (field_a, field_b, field_c):
                #   WHERE (field_a < ?)
                #    OR (field_a = ? AND field_b < ?)
                #    OR (field_a = ? AND field_b = ? AND field_c < ?)
                where_clauses = []
                for ix, field_name in enumerate(self._quoted_key_field_names):
                    clauses = []
                    for jx, prior_field_name in enumerate(self._quoted_key_field_names[0:ix]):
                        clauses.append(f'{prior_field_name} = ?')
                    clauses.append(f'{field_name} < ?')
                    where_clauses.append(f"({' AND '.join(clauses)})")

                self._snapshot_rows_query = constants.SNAPSHOT_ROWS_QUERY_TEMPLATE.format(
                    fields=select_column_specs, schema_name=self.schema_name,
                    table_name=self.table_name, where_spec=' OR '.join(where_clauses),
                    order_spec=', '.join([f'{x} DESC' for x in self._quoted_key_field_names])
                )

                if start_from_key_for_snapshot:
                    key_min = self._get_min_key_value()

                    if key_min:
                        if tuple(str(x) for x in key_min) == tuple(str(x) for x in start_from_key_for_snapshot):
                            self.snapshot_complete = True
                        else:
                            self._last_read_key_for_snapshot = start_from_key_for_snapshot
                    else:
                        self._last_read_key_for_snapshot = start_from_key_for_snapshot
                else:
                    key_max = self._get_max_key_value()
                    if key_max:
                        logger.info('Table %s is starting a full snapshot, working back from max key (%s)',
                                    self.fq_name,
                                    ', '.join([f'{k}: {v}' for k, v in zip(self._key_field_names, key_max)]))

                        self._initial_snapshot_rows_query = constants.SNAPSHOT_ROWS_QUERY_TEMPLATE.format(
                            fields=select_column_specs, schema_name=self.schema_name,
                            table_name=self.table_name, where_spec='1=1',
                            order_spec=', '.join([f'{x} DESC' for x in self._quoted_key_field_names])
                        )

                        self._last_read_key_for_snapshot = NEW_SNAPSHOT
                    else:
                        logger.warning('Snapshot was requested for table %s but it appears empty.', self.fq_name)
                        self.snapshot_complete = True
            else:
                raise Exception(
                    f"Snapshotting was requested for table {self.fq_name}, but it does not appear to have a primary "
                    f"key (which is required for snapshotting at this time). You can get past this error by adding"
                    f"the table to the snapshot blacklist")

        TrackedTable._REGISTERED_TABLES_BY_KAFKA_TOPIC[self.topic_name] = self

    def _message_kv_from_change_row(self, change_row: Tuple) -> Tuple[Dict[str, object], Dict[str, object]]:
        meta_cols, table_cols = change_row[:constants.CDC_METADATA_COL_COUNT], \
                                change_row[constants.CDC_METADATA_COL_COUNT:]
        key, value = {}, {}

        if self._has_pk:
            for field in self._key_fields:
                key[field.name] = field.transform_fn(table_cols[field.change_table_ordinal - 1])
        else:
            # CAUTION: this strategy for handling PK-less tables means that if columns are added or removed from the
            # capture instance in the future, the key value computed for the same source table row will change:
            m = hashlib.md5()
            m.update(str(zip(self._value_field_names, table_cols)).encode('utf8'))
            row_hash = uuid.uuid5(uuid.UUID(bytes=m.digest()), self.fq_name)
            key[constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT] = row_hash

        for ix, field in enumerate(self._value_fields):
            value[field.name] = field.transform_fn(table_cols[ix])

        value[constants.LSN_NAME] = meta_cols[constants.LSN_POS]
        value[constants.SEQVAL_NAME] = meta_cols[constants.SEQVAL_POS]
        value[constants.OPERATION_NAME] = constants.CDC_OPERATION_ID_TO_NAME[meta_cols[constants.OPERATION_POS]]
        value[constants.TRAN_END_TIME_NAME] = meta_cols[constants.TRAN_END_TIME_POS].isoformat()

        if meta_cols[constants.OPERATION_POS] == constants.SNAPSHOT_OPERATION_ID:
            value['_cdc_updated_fields'] = self._value_field_names
        else:
            value['_cdc_updated_fields'] = list(self._updated_cols_from_mask(meta_cols[constants.UPDATE_MASK_POS]))

        return key, value

    def _updated_cols_from_mask(self, cdc_update_mask: bytes) -> List[str]:
        pos = 0
        for byte in reversed(cdc_update_mask):
            for j in range(8):
                if (byte & (1 << j)) >> j:
                    yield self._value_field_names[pos]
                pos += 1

    def _get_db_time(self):
        if (datetime.datetime.now() - TrackedTable._DB_TIME_DELTA_LAST_REFRESH) > datetime.timedelta(minutes=1):
            self._cursor.execute('SELECT GETDATE()')
            TrackedTable._DB_TIME_DELTA = self._cursor.fetchval() - datetime.datetime.now()
            TrackedTable._DB_TIME_DELTA_LAST_REFRESH = datetime.datetime.now()
            logger.debug('Current DB time delta: %s', TrackedTable._DB_TIME_DELTA)
        return datetime.datetime.now() + TrackedTable._DB_TIME_DELTA

    # This method's Return value is used to order results in the priority queue used to determine the sequence in
    # which rows are published.
    def _get_queue_priority_tuple(self, row) -> Tuple[datetime.datetime, bytes, bytes, int, str]:
        if row is None:
            return (self._get_db_time() + constants.DB_TABLE_POLL_INTERVAL,  # defer until ready for next poll
                    constants.BEGINNING_CHANGE_TABLE_INDEX.lsn,
                    constants.BEGINNING_CHANGE_TABLE_INDEX.seqval,
                    0,
                    self.fq_name)

        if row[constants.OPERATION_POS] == constants.SNAPSHOT_OPERATION_ID:
            return (row[constants.TRAN_END_TIME_POS],  # for snapshot rows this is just GETDATE() from that query
                    constants.BEGINNING_CHANGE_TABLE_INDEX.lsn,
                    constants.BEGINNING_CHANGE_TABLE_INDEX.seqval,
                    constants.SNAPSHOT_OPERATION_ID,
                    self.fq_name)

        return (row[constants.TRAN_END_TIME_POS],
                row[constants.LSN_POS],
                row[constants.SEQVAL_POS],
                row[constants.OPERATION_POS],
                self.fq_name)

    def _maybe_refresh_buffer(self) -> None:
        if len(self._row_buffer) > 0:
            return

        if not self._lagging and (datetime.datetime.now() - self._last_db_poll_time) < constants.DB_TABLE_POLL_INTERVAL:
            return

        change_rows_read_ctr, snapshot_rows_read_ctr = 0, 0

        lsn = self._last_read_change_table_index.lsn
        seqval = self._last_read_change_table_index.seqval
        operation = self._last_read_change_table_index.operation

        change_row_query_params = (constants.DB_ROW_BATCH_SIZE, lsn, lsn, seqval, lsn, seqval, operation)
        logger.debug('Polling DB for capture instance %s', self.capture_instance_name)

        self._cursor.execute(self._change_rows_query, change_row_query_params)
        change_row = None
        for change_row in self._cursor.fetchall():
            change_rows_read_ctr += 1
            self._row_buffer.append(change_row)

        self._last_db_poll_time = datetime.datetime.now()

        if change_row:
            logger.debug('Retrieved %s change rows', change_rows_read_ctr)
            self._last_read_change_table_index = ChangeTableIndex(
                change_row[constants.LSN_POS], change_row[constants.SEQVAL_POS], change_row[constants.OPERATION_POS])

        if self.snapshot_allowed and not self.snapshot_complete:
            snapshot_query_params = [constants.DB_ROW_BATCH_SIZE]
            logger.debug('Polling DB for snapshot rows from %s', self.fq_name)

            if self._last_read_key_for_snapshot == NEW_SNAPSHOT:
                self._cursor.execute(self._initial_snapshot_rows_query, snapshot_query_params)
            else:
                for key_field_pos in range(len(self._last_read_key_for_snapshot)):
                    snapshot_query_params.extend(self._last_read_key_for_snapshot[:key_field_pos + 1])
                self._cursor.execute(self._snapshot_rows_query, snapshot_query_params)

            snapshot_row = None
            for snapshot_row in self._cursor.fetchall():
                snapshot_rows_read_ctr += 1
                self._row_buffer.append(snapshot_row)

            if snapshot_row:
                self._last_read_key_for_snapshot = tuple(snapshot_row[n - 1 + constants.CDC_METADATA_COL_COUNT]
                                                         for n in self._key_field_positions)
                logger.debug('Retrieved %s snapshot rows', snapshot_rows_read_ctr)
            else:
                logger.info("SNAPSHOT COMPLETED for table %s. Last read key: (%s)", self.fq_name, ', '.join(
                    [f'{k}: {v}' for k, v in zip(self._key_field_names, self._last_read_key_for_snapshot)]))
                self._last_read_key_for_snapshot = None
                self.snapshot_complete = True

        self._lagging = change_rows_read_ctr == constants.DB_ROW_BATCH_SIZE or snapshot_rows_read_ctr

    def _get_max_key_value(self):
        order_by_spec = ", ".join([f'{fn} DESC' for fn in self._quoted_key_field_names])

        with self.db_conn.cursor() as cursor:
            cursor.execute(f'SELECT TOP 1 {", ".join(self._quoted_key_field_names)} '
                           f'FROM [{self.schema_name}].[{self.table_name}] ORDER BY {order_by_spec}')
            return cursor.fetchone()

    def _get_min_key_value(self):
        order_by_spec = ", ".join([f'{fn} ASC' for fn in self._quoted_key_field_names])

        with self.db_conn.cursor() as cursor:
            cursor.execute(f'SELECT TOP 1 {", ".join(self._quoted_key_field_names)} '
                           f'FROM [{self.schema_name}].[{self.table_name}] ORDER BY {order_by_spec}')
            return cursor.fetchone()


# This pulls the "greatest" capture instance running for each source table, in the event there is more than one.
def get_latest_capture_instance_names(db_conn: pyodbc.Connection, capture_instance_version_strategy: str,
                                      capture_instance_version_regex: str) -> List[str]:
    if capture_instance_version_strategy == 'regex' and not capture_instance_version_regex:
        raise Exception('Please provide a capture_instance_version_regex when specifying the `regex` '
                        'capture_instance_version_strategy.')
    result = []
    table_to_capture_instances = collections.defaultdict(list)
    capture_instance_version_regex = capture_instance_version_regex and re.compile(capture_instance_version_regex)

    with db_conn.cursor() as cursor:
        cursor.execute(constants.CDC_CAPTURE_INSTANCES_QUERY)
        for row in cursor.fetchall():
            as_dict = {
                'source_object_id': row[0],
                'capture_instance': row[1],
                'create_date': row[2],
            }
            if capture_instance_version_regex:
                match = capture_instance_version_regex.match(row[1])
                as_dict['regex_matched_group'] = match and match.group(1) or ''
            table_to_capture_instances[as_dict['source_object_id']].append(as_dict)

    for source_id, capture_instances in table_to_capture_instances.items():
        if capture_instance_version_strategy == 'create_date':
            latest_instance = sorted(capture_instances, key=lambda x: x['create_date'])[-1]
        elif capture_instance_version_strategy == 'regex':
            latest_instance = sorted(capture_instances, key=lambda x: x['regex_matched_group'])[-1]
        else:
            raise Exception(f'Capture instance version strategy "{capture_instance_version_strategy}" not recognized.')
        result.append(latest_instance['capture_instance'])

    return result


def build_tracked_tables_from_cdc_metadata(
        db_conn: pyodbc.Connection, topic_name_template: str, table_whitelist_regex: str,
        table_blacklist_regex: str, snapshot_table_whitelist_regex: str, snapshot_table_blacklist_regex: str,
        capture_instance_version_strategy: str, capture_instance_version_regex: str) -> List[TrackedTable]:
    result = []

    latest_names = get_latest_capture_instance_names(
        db_conn, capture_instance_version_strategy, capture_instance_version_regex)
    logger.debug('Latest capture instance names determined by "%s" strategy: %s', capture_instance_version_strategy,
                 sorted(latest_names))

    table_whitelist_regex = table_whitelist_regex and re.compile(
        table_whitelist_regex, re.IGNORECASE)
    table_blacklist_regex = table_blacklist_regex and re.compile(
        table_blacklist_regex, re.IGNORECASE)
    snapshot_table_whitelist_regex = snapshot_table_whitelist_regex and re.compile(
        snapshot_table_whitelist_regex, re.IGNORECASE)
    snapshot_table_blacklist_regex = snapshot_table_blacklist_regex and re.compile(
        snapshot_table_blacklist_regex, re.IGNORECASE)

    meta_query = constants.CDC_METADATA_QUERY.replace('?', ', '.join(['?'] * len(latest_names)))
    name_to_meta_fields = collections.defaultdict(list)

    with db_conn.cursor() as cursor:
        cursor.execute(meta_query, latest_names)
        for row in cursor.fetchall():
            # 0:4 gets schema name, table name, capture instance name, min captured LSN:
            name_to_meta_fields[tuple(row[0:4])].append(row[4:])

    for (schema_name, table_name, capture_instance_name, min_lsn), fields in name_to_meta_fields.items():
        fq_table_name = f'{schema_name}.{table_name}'

        can_snapshot = False

        if table_whitelist_regex and not table_whitelist_regex.match(fq_table_name):
            logger.debug('Table %s excluded by whitelist', fq_table_name)
            continue

        if table_blacklist_regex and table_blacklist_regex.match(fq_table_name):
            logger.debug('Table %s excluded by blacklist', fq_table_name)
            continue

        if snapshot_table_whitelist_regex and snapshot_table_whitelist_regex.match(fq_table_name):
            logger.debug('Table %s WILL be snapshotted due to whitelisting', fq_table_name)
            can_snapshot = True

        if snapshot_table_blacklist_regex and snapshot_table_blacklist_regex.match(fq_table_name):
            logger.debug('Table %s will NOT be snapshotted due to blacklisting', fq_table_name)
            can_snapshot = False

        topic_name = topic_name_template.format(
            schema_name=schema_name, table_name=table_name, capture_instance_name=capture_instance_name)

        tracked_table = TrackedTable(db_conn, schema_name, table_name, capture_instance_name, topic_name,
                                     min_lsn, can_snapshot)

        for (change_table_ordinal, column_name, sql_type_name, primary_key_ordinal, decimal_precision,
             decimal_scale) in fields:
            tracked_table.add_field(TrackedField(column_name, sql_type_name, change_table_ordinal,
                                                 primary_key_ordinal, decimal_precision, decimal_scale))

        result.append(tracked_table)

    return result
