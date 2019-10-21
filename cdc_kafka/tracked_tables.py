import collections
import datetime
import hashlib
import json
import logging
import re
import uuid
from functools import total_ordering
from typing import Tuple, Dict, List, Union, Any

import avro
import confluent_kafka
import pyodbc

from . import avro_from_sql, constants

logger = logging.getLogger(__name__)


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
        self.name = name
        self.sql_type_name = sql_type_name
        self.change_table_ordinal = change_table_ordinal
        self.primary_key_ordinal = primary_key_ordinal
        self.avro_schema = avro_from_sql.avro_schema_from_sql_type(
            name, sql_type_name, decimal_precision, decimal_scale, False)
        self.nullable_avro_schema = avro_from_sql.avro_schema_from_sql_type(
            name, sql_type_name, decimal_precision, decimal_scale, True)
        self.transform_fn = avro_from_sql.avro_transform_fn_from_sql_type(sql_type_name)


class TrackedTable(object):
    _registered_tables_by_kafka_topic: Dict[str, 'TrackedTable'] = {}

    @staticmethod
    def progress_message_extractor(topic_name: str, msg_value: Dict[str, Any]) -> \
            Tuple[Dict[str, Any], avro.schema.RecordSchema, Dict[str, Any], avro.schema.RecordSchema]:

        table = TrackedTable._registered_tables_by_kafka_topic[topic_name]
        key = {"topic_name": topic_name, "capture_instance_name": table.capture_instance_name}

        if msg_value['_cdc_operation'] == constants.SNAPSHOT_OPERATION_NAME:
            key['progress_kind'] = constants.SNAPSHOT_ROWS_PROGRESS_KIND
            value = {
                'last_published_snapshot_key_field_values': [{
                    'field_name': f.name,
                    'sql_type': f.sql_type_name,
                    'value_as_string': str(msg_value[f.name])
                } for f in table.key_fields]
            }
        else:
            key['progress_kind'] = constants.CHANGE_ROWS_PROGRESS_KIND
            value = {
                'last_published_change_table_lsn': msg_value['_cdc_start_lsn'],
                'last_published_change_table_seqval': msg_value['_cdc_seqval'],
                'last_published_change_table_operation': constants.CDC_OPERATION_NAME_TO_ID[msg_value['_cdc_operation']]
            }

        return key, constants.PROGRESS_MESSAGE_AVRO_KEY_SCHEMA, value, constants.PROGRESS_MESSAGE_AVRO_VALUE_SCHEMA

    def __init__(self, db_conn, schema_name: str, table_name: str, capture_instance_name: str, topic_name: str,
                 snapshot_allowed: bool):
        self.db_conn = db_conn
        self.schema_name = schema_name
        self.table_name = table_name
        self.capture_instance_name = capture_instance_name
        self.topic_name = topic_name
        self.snapshot_allowed = snapshot_allowed

        self.fq_name = f'{self.schema_name}.{self.table_name}'

        # The below properties are not set until `finalize_table` is called:
        self.key_fields = None
        self.key_field_names = None
        self.key_field_positions = None
        self.quoted_key_field_names = None
        self.value_fields = None
        self.value_field_names = None
        self.key_schema = None
        self.value_schema = None
        self.change_rows_query = None
        self.snapshot_rows_query = None
        self.snapshot_query = None
        self.last_read_change_table_index = None
        self.last_read_key_for_snapshot = None

        self._fields_added_pending_finalization = []
        self._finalized = False
        self._has_pk = None
        self._row_buffer = collections.deque()
        self._buffer_queries_cursor = self.db_conn.cursor()
        self._last_db_poll_time = constants.BEGINNING_DATETIME
        self._lagging = False

    def pop_next(self) -> Tuple[Tuple, Union[Dict, None], Union[Dict, None]]:
        self._maybe_refresh_buffer()

        if len(self._row_buffer) == 0:
            return self._get_queue_priority_tuple(None), None, None

        next_row = self._row_buffer.popleft()
        msg_key, msg_value = self._message_kv_from_change_row(next_row)
        return self._get_queue_priority_tuple(next_row), msg_key, msg_value

    def add_field(self, field: TrackedField) -> None:
        self._fields_added_pending_finalization.append(field)

    # 'Finalizing' mostly means doing the things we can't do until we know all of the table's fields have been added
    def finalize_table(self, start_after_change_table_index: ChangeTableIndex,
                       start_from_key_for_snapshot: Tuple) -> None:
        if self._finalized:
            raise Exception(f"Attempted to finalize table {self.fq_name} more than once")

        self._finalized = True

        if start_after_change_table_index.lsn > constants.BEGINNING_CHANGE_TABLE_INDEX.lsn:
            with self.db_conn.cursor() as cursor:
                cursor.execute(constants.CDC_EARLIEST_LSN_QUERY, self.capture_instance_name)
                earliest_lsn_available_in_db = cursor.fetchval()

            if earliest_lsn_available_in_db > start_after_change_table_index.lsn:
                # TODO - this should maybe trigger a re-snapshot. For now just bail.
                raise Exception(
                    f'The earliest change LSN available in the DB for capture instance {self.capture_instance_name} '
                    f'(0x{earliest_lsn_available_in_db.hex()}) is later than the log position we need to start from '
                    f'based on the prior progress stored in Kafka ((0x{start_after_change_table_index.lsn.hex()}). '
                    f'Cannot continue.')

        self.last_read_change_table_index = start_after_change_table_index

        self.value_fields = sorted(self._fields_added_pending_finalization, key=lambda f: f.change_table_ordinal)
        self.value_field_names = [f.name for f in self.value_fields]

        self._fields_added_pending_finalization = None

        key_fields = [f for f in self.value_fields if f.primary_key_ordinal is not None]
        if not key_fields:
            self._has_pk = False
            key_fields = [TrackedField(
                constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT, 'varchar', 0, 0, 0, 0)]
        else:
            self._has_pk = True

        self.key_fields = sorted(key_fields, key=lambda f: f.primary_key_ordinal)
        self.key_field_names = [f.name for f in self.key_fields]
        self.key_field_positions = [f.change_table_ordinal for f in self.key_fields]
        self.quoted_key_field_names = [f'[{f.name}]' for f in self.key_fields]

        self.key_schema = confluent_kafka.avro.loads(json.dumps({
            "name": f"{self.schema_name}_{self.table_name}_cdc__key",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": [f.avro_schema for f in self.key_fields]
        }))

        value_fields_plus_metadata_fields = avro_from_sql.get_cdc_metadata_fields_avro_schemas(
            self.value_field_names) + [f.nullable_avro_schema for f in self.value_fields]

        self.value_schema = confluent_kafka.avro.loads(json.dumps({
            "name": f"{self.schema_name}_{self.table_name}_cdc__value",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": value_fields_plus_metadata_fields
        }))

        select_column_specs = ', '.join([f'ct.[{f}]' for f in self.value_field_names])

        self.change_rows_query = constants.CHANGE_ROWS_QUERY_TEMPLATE.format(
            fields=select_column_specs, capture_instance_name=self.capture_instance_name)

        if self.snapshot_allowed:
            if self._has_pk:
                where_clauses = []
                for ix, field_name in enumerate(self.quoted_key_field_names):
                    clauses = []
                    for jx, prior_field_name in enumerate(self.quoted_key_field_names[0:ix]):
                        clauses.append(f'{prior_field_name} = ?')
                    clauses.append(f'{field_name} < ?')
                    where_clauses.append(' AND '.join(clauses))
                self.snapshot_rows_query = constants.SNAPSHOT_ROWS_QUERY_TEMPLATE.format(
                    fields=select_column_specs, schema_name=self.schema_name,
                    table_name=self.table_name, where_spec=' OR '.join(where_clauses),
                    order_spec=', '.join([f'{x} DESC' for x in self.quoted_key_field_names])
                )
                self.last_read_key_for_snapshot = \
                    start_from_key_for_snapshot or self._get_max_key_value()
            else:
                logger.error(
                    f"Snapshotting was requested for table {self.fq_name}, but it does not appear to have a primary "
                    f"key (which is required for snapshotting at this time).")
                # raise Exception(
                #     f"Snapshotting was requested for table {self.fq_name}, but it does not appear to have a primary "
                #     f"key (which is required for snapshotting at this time).")

            TrackedTable._registered_tables_by_kafka_topic[self.topic_name] = self

    def _message_kv_from_change_row(self, change_row: Tuple) -> Tuple[Dict[str, object], Dict[str, object]]:
        meta_cols, table_cols = change_row[:constants.NBR_METADATA_COLS], change_row[constants.NBR_METADATA_COLS:]
        key, value = {}, {}

        if self._has_pk:
            for field in self.key_fields:
                key[field.name] = field.transform_fn(table_cols[field.change_table_ordinal - 1])
        else:
            # CAUTION: this strategy for handling PK-less tables means that if columns are added or removed from the
            # capture instance in the future, the key value computed for the same source table row will change:
            m = hashlib.md5()
            m.update(str(zip(self.value_field_names, table_cols)).encode('utf8'))
            row_hash = uuid.uuid5(uuid.UUID(bytes=m.digest()), self.fq_name)
            key[constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT] = row_hash

        for ix, field in enumerate(self.value_fields):
            value[field.name] = field.transform_fn(table_cols[ix])

        value['_cdc_start_lsn'] = meta_cols[constants.LSN_POS]
        value['_cdc_seqval'] = meta_cols[constants.SEQVAL_POS]
        value['_cdc_operation'] = constants.CDC_OPERATION_ID_TO_NAME[meta_cols[constants.OPERATION_POS]]
        value['_cdc_tran_end_time'] = meta_cols[constants.TRAN_END_TIME_POS].isoformat()

        # The mask is null when this is a snapshot row:
        value['_cdc_updated_fields'] = self.value_field_names if meta_cols[constants.UPDATE_MASK_POS] is None \
            else list(self._updated_cols_from_mask(meta_cols[constants.UPDATE_MASK_POS]))

        return key, value

    def _updated_cols_from_mask(self, cdc_update_mask: bytes) -> List[str]:
        pos = 0
        for byte in reversed(cdc_update_mask):
            for j in range(8):
                if (byte & (1 << j)) >> j:
                    yield self.value_field_names[pos]
                pos += 1

    # Return value is used to order results in the priority queue used to determine the sequence in which rows
    # are published.
    def _get_queue_priority_tuple(self, row) -> Tuple:
        if row is None:
            return (self._last_db_poll_time + constants.DB_TABLE_POLL_INTERVAL,  # defer until ready for next poll
                    constants.BEGINNING_CHANGE_TABLE_INDEX.lsn,
                    constants.BEGINNING_CHANGE_TABLE_INDEX.seqval,
                    0,
                    self.fq_name)

        if row[constants.OPERATION_POS] == constants.SNAPSHOT_OPERATION_ID:
            return (row[constants.TRAN_END_TIME_POS],  # for snapshot rows this is just GETDATE() from that query
                    constants.BEGINNING_CHANGE_TABLE_INDEX.lsn,
                    constants.BEGINNING_CHANGE_TABLE_INDEX.seqval,
                    0,
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

        lsn = '0x' + self.last_read_change_table_index.lsn.hex()
        seqval = '0x' + self.last_read_change_table_index.seqval.hex()
        operation = self.last_read_change_table_index.operation

        change_row_query_params = (constants.DB_ROW_BATCH_SIZE, lsn, lsn, seqval, lsn, seqval, operation)

        logger.debug('Polling DB for capture instance %s', self.capture_instance_name)

        change_rows_read_ctr = 0
        self._buffer_queries_cursor.execute(self.change_rows_query, change_row_query_params)

        for change_row in self._buffer_queries_cursor.fetchall():
            change_rows_read_ctr += 1
            last_lsn = change_row[constants.LSN_POS]
            last_seqval = change_row[constants.SEQVAL_POS]
            last_operation = change_row[constants.OPERATION_POS]
            self._row_buffer.append(change_row)

        self._last_db_poll_time = datetime.datetime.now()

        if change_rows_read_ctr > 0:
            self.last_read_change_table_index = ChangeTableIndex(last_lsn, last_seqval, last_operation)
            logger.debug('Retrieved %s change rows', change_rows_read_ctr)

        if self.last_read_key_for_snapshot and change_rows_read_ctr < constants.DB_ROW_BATCH_SIZE:
            snapshot_query_params = [constants.DB_ROW_BATCH_SIZE - change_rows_read_ctr]
            for key_field_pos in range(len(self.last_read_key_for_snapshot)):
                snapshot_query_params.extend(self.last_read_key_for_snapshot[:key_field_pos + 1])
            logger.debug('Polling DB for snapshot rows from %s', self.fq_name)

            snapshot_rows_read_ctr = 0
            self._buffer_queries_cursor.execute(self.snapshot_rows_query, snapshot_query_params)

            for snapshot_row in self._buffer_queries_cursor.fetchall():
                snapshot_rows_read_ctr += 1
                self.last_read_key_for_snapshot = tuple(snapshot_row[n + constants.NBR_METADATA_COLS]
                                                        for n in self.key_field_positions)
                self._row_buffer.append(snapshot_row)

            if snapshot_rows_read_ctr > 0:
                logger.debug('Retrieved %s snapshot rows', snapshot_rows_read_ctr)
            else:
                logger.info("SNAPSHOT COMPLETED for table %s. Last read key: (%s)", self.fq_name, ', '.join(
                    [f'{k}: {v}' for k, v in zip(self.key_field_names, self.last_read_key_for_snapshot)]))
                self.last_read_key_for_snapshot = None

        self._lagging = change_rows_read_ctr == constants.DB_ROW_BATCH_SIZE

    def _get_max_key_value(self):
        order_by_spec = ", ".join([f'{fn} DESC' for fn in self.quoted_key_field_names])

        with self.db_conn.cursor() as cursor:
            cursor.execute(f'SELECT TOP 1 {", ".join(self.quoted_key_field_names)} '
                           f'FROM [{self.schema_name}].[{self.table_name}] ORDER BY {order_by_spec}')
            key_max = cursor.fetchone()
        if key_max:
            logger.info('Table %s is starting a full snapshot, working back from max key (%s)', self.fq_name,
                        ', '.join([f'{k}: {v}' for k, v in zip(self.key_field_names, key_max)]))
        else:
            logger.warning('Snapshot was requested for table %s but it appears empty.', self.fq_name)

        return key_max


# This pulls the "greatest" capture instance running for each source table, in the event there is more than one.
def get_latest_capture_instance_names(db_conn: pyodbc.Connection, capture_instance_version_strategy: str,
                                      capture_instance_version_regex: str) -> List[str]:
    if capture_instance_version_strategy == 'regex' and not capture_instance_version_regex:
        raise Exception('Please provide a capture_instance_version_regex when specifying the `regex` '
                        'capture_instance_version_strategy.')
    result = []
    tables = collections.defaultdict(list)
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
                as_dict['regex_matched_group'] = capture_instance_version_regex.match(row['capture_instance']).group(1)
            tables[as_dict['source_object_id']].append(as_dict)

    for source_id, capture_instances in tables.items():
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

    latest_names = get_latest_capture_instance_names(db_conn, capture_instance_version_strategy,
                                                     capture_instance_version_regex)
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
            name_to_meta_fields[tuple(row[0:3])].append(row[3:])

    for (schema_name, table_name, capture_instance_name), fields in name_to_meta_fields.items():
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

        tracked_table = TrackedTable(db_conn, schema_name, table_name, capture_instance_name, topic_name, can_snapshot)

        for (change_table_ordinal, column_name, sql_type_name, primary_key_ordinal, decimal_precision,
             decimal_scale) in fields:
            tracked_table.add_field(TrackedField(column_name, sql_type_name, change_table_ordinal,
                                                 primary_key_ordinal, decimal_precision, decimal_scale))

        result.append(tracked_table)

    return result
