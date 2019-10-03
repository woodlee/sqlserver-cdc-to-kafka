import collections
import datetime
import hashlib
import json
import logging
import re
import uuid
from functools import total_ordering
from typing import Tuple, Dict, List, Union

import confluent_kafka
import sqlalchemy
from sqlalchemy import text

from . import avro_from_sql, constants

logger = logging.getLogger(__name__)

# SQL Server CDC capture tables have a single compound index on (lsn, seqval, operation)
@total_ordering
class ChangeTableIndex(object):
    def __init__(self, lsn: bytes, seqval: bytes, operation_id: int):
        self.lsn: bytes = lsn
        self.seqval: bytes = seqval
        if isinstance(operation_id, int):
            self.operation_id: int = operation_id
        else:
            self.operation_id: int = constants.CDC_OPERATION_NAME_TO_ID[operation_id]

    def __eq__(self, other) -> bool:
        return self.lsn + self.seqval + bytes([self.operation_id]) == \
               other.lsn + other.seqval + bytes([other.operation_id])

    def __lt__(self, other) -> bool:
        return self.lsn + self.seqval + bytes([self.operation_id]) < \
               other.lsn + other.seqval + bytes([other.operation_id])

    def __repr__(self) -> str:
        return f'0x{self.lsn.hex()}:0x{self.seqval.hex()}:{self.operation_id}'


class TrackedField(object):
    def __init__(self, name: str, sql_type_name: str, change_table_ordinal: int, primary_key_ordinal: int,
                 decimal_precision: int, decimal_scale: int, is_nullable: bool, is_identity: bool):
        self.name = name
        self.change_table_ordinal = change_table_ordinal
        self.primary_key_ordinal = primary_key_ordinal
        self.is_auto_incrementing = is_identity and not is_nullable
        self.avro_schema = avro_from_sql.avro_schema_from_sql_type(
            name, sql_type_name, decimal_precision, decimal_scale)
        self.transform_fn = avro_from_sql.avro_transform_fn_from_sql_type(sql_type_name)


class TrackedTable(object):
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
        self.value_fields = None
        self.value_field_names = None
        self.key_schema = None
        self.value_schema = None
        self.change_rows_query = None
        self.snapshot_query = None
        self.incrementing_column_name = None
        self.last_read_change_table_index = None
        self.last_read_change_time = None

        self._added_fields = []
        self._finalized = False
        self._has_pk = None
        self._change_row_buffer = collections.deque()
        self._last_poll_time = constants.BEGINNING_DATETIME
        self._lagging = False

    def pop_next(self) -> Union[None, Tuple]:
        self._refresh_buffer()
        if len(self._change_row_buffer) > 0:
            change_row = self._change_row_buffer.popleft()
            index = ChangeTableIndex(change_row['_cdc_start_lsn'], change_row['_cdc_seqval'],
                                     change_row['_cdc_operation'])
            return (change_row['_cdc_tran_end_time'], index, *self._message_kv_from_change_row(change_row))
        return None

    def _refresh_buffer(self) -> None:
        if len(self._change_row_buffer) > constants.CHANGE_ROW_BATCH_SIZE / 10:
            return

        if not self._lagging and (datetime.datetime.now() - self._last_poll_time) < constants.TABLE_POLL_INTERVAL:
            return

        params = {'number_to_get': constants.CHANGE_ROW_BATCH_SIZE,
                  'lsn': self.last_read_change_table_index.lsn,
                  'seqval': self.last_read_change_table_index.seqval,
                  'operation_id': self.last_read_change_table_index.operation_id}

        logger.debug('Polling capture instance %s', self.capture_instance_name)

        rows = self.db_conn.execute(self.change_rows_query, params)
        self._last_poll_time = datetime.datetime.now()

        row_ctr = 0
        for row in rows:
            self._change_row_buffer.append(row)
            row_ctr += 1

        if row_ctr > 0:
            self.last_read_change_table_index = ChangeTableIndex(
                row['_cdc_start_lsn'], row['_cdc_seqval'], row['_cdc_operation'])
            self.last_read_change_time = row['_cdc_tran_end_time']
            logger.debug('Retrieved %s change rows', row_ctr)

        self._lagging = row_ctr == constants.CHANGE_ROW_BATCH_SIZE

    def add_field(self, field: TrackedField) -> None:
        self._added_fields.append(field)

    # 'Finalizing' mostly means doing the things we can't do until we know all of the table's fields have been added
    def finalize_table(self, start_after_change_table_index: ChangeTableIndex,
                       start_after_change_time: datetime) -> None:
        if self._finalized:
            raise Exception(f"Attempted to finalized table {self.fq_name} more than once")

        self._finalized = True

        if start_after_change_table_index.lsn > constants.BEGINNING_CHANGE_TABLE_INDEX.lsn:
            earliest_lsn_available = self.db_conn.scalar(
                text(constants.CDC_EARLIEST_LSN_QUERY), {'capture_instance': self.capture_instance_name})

            if earliest_lsn_available > start_after_change_table_index.lsn:
                # TODO - this should maybe trigger a re-snapshot. For now just bail.
                raise Exception(f'The earliest change LSN available in the DB for capture instance '
                                f'{self.capture_instance_name} (0x{earliest_lsn_available.hex()}) is later than the '
                                f'log position we need to start from based on the last published messages found in '
                                f'Kafka ((0x{start_after_change_table_index.lsn.hex()}). Cannot continue.')

        self.last_read_change_table_index = start_after_change_table_index
        self.last_read_change_time = start_after_change_time

        self.value_fields = sorted(self._added_fields, key=lambda f: f.change_table_ordinal)
        self.value_field_names = [f.name for f in self.value_fields]

        self._added_fields = None

        key_fields = [f for f in self.value_fields if f.primary_key_ordinal is not None]
        if not key_fields:
            self._has_pk = False
            key_fields = [TrackedField(
                constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT, 'varchar', 0, 0, 0, 0, False, False)]
        else:
            self._has_pk = True

        self.key_fields = sorted(key_fields, key=lambda f: f.primary_key_ordinal)

        self.key_schema = confluent_kafka.avro.loads(json.dumps({
            "name": f"{self.schema_name}_{self.table_name}_cdc_key",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": [f.avro_schema for f in self.key_fields]
        }))

        value_fields_plus_metadata_fields = avro_from_sql.get_cdc_metadata_fields_avro_schemas(
            self.value_field_names) + [f.avro_schema for f in self.value_fields]

        self.value_schema = confluent_kafka.avro.loads(json.dumps({
            "name": f"{self.schema_name}_{self.table_name}_cdc_value",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": value_fields_plus_metadata_fields
        }))

        select_column_specs = '],ct.['.join(self.value_field_names)

        self.change_rows_query = text(constants.CDC_CHANGE_ROWS_QUERY_TEMPLATE.format(
            batch_size=constants.CHANGE_ROW_BATCH_SIZE, fields=select_column_specs,
            capture_instance_name=self.capture_instance_name))

        autoinc_fields = [f for f in self.value_fields if f.is_auto_incrementing]

        if len(autoinc_fields) == 1:
            self.incrementing_column_name = autoinc_fields[0].name

        if self.incrementing_column_name:
            self.snapshot_query = text(constants.SNAPSHOT_ROWS_QUERY_TEMPLATE.format(
                batch_size=constants.CHANGE_ROW_BATCH_SIZE, fields=select_column_specs, schema_name=self.schema_name,
                table_name=self.table_name, incrementing_column=self.incrementing_column_name
            ))
        elif self.snapshot_allowed:
            raise Exception(f"Snapshotting was requested for table {self.fq_name}, but it does not appear to have an "
                            "auto-incrementing identity column (which is required for snapshotting at this "
                            "time).")

    def _message_kv_from_change_row(self, change_row: sqlalchemy.engine.RowProxy) \
            -> Tuple[Dict[str, object], Dict[str, object]]:
        key, value = {}, {}

        if self._has_pk:
            for field in self.key_fields:
                key[field.name] = field.transform_fn(change_row[field.name])
        else:
            # CAUTION: this strategy for handling PK-less tables means that if columns are added or removed from the
            # capture instance in the future, the key value computed for the same source table row will change:
            m = hashlib.md5()
            fields_minus_meta = [(fn.lower(), change_row[fn]) for fn in self.value_field_names]
            m.update(str(fields_minus_meta).encode('utf8'))
            row_hash = uuid.uuid5(uuid.UUID(bytes=m.digest()), self.fq_name)
            key[constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT] = row_hash

        for field in self.value_fields:
            value[field.name] = field.transform_fn(change_row[field.name])

        value['_cdc_start_lsn'] = change_row['_cdc_start_lsn']
        value['_cdc_seqval'] = change_row['_cdc_seqval']
        value['_cdc_operation'] = constants.CDC_OPERATION_ID_TO_NAME[change_row['_cdc_operation']]
        value['_cdc_tran_end_time'] = change_row['_cdc_tran_end_time'].isoformat()
        value['_cdc_updated_fields'] = list(self._updated_cols_from_mask(change_row['_cdc_update_mask']))

        return key, value

    def _updated_cols_from_mask(self, cdc_update_mask: bytes) -> List[str]:
        pos = 0
        for byte in reversed(cdc_update_mask):
            for j in range(8):
                if (byte & (1 << j)) >> j:
                    yield self.value_field_names[pos]
                pos += 1


# This pulls the "greatest" capture instance running for each source table, in the event there is more than one.
def get_latest_capture_instance_names(db_conn: sqlalchemy.engine.Connection, capture_instance_version_strategy: str,
                                      capture_instance_version_regex: str) -> List[str]:
    if capture_instance_version_strategy == 'regex' and not capture_instance_version_regex:
        raise Exception('Please provide a capture_instance_version_regex when specifying the `regex` '
                        'capture_instance_version_strategy.')
    result = []
    rows = db_conn.execute(text(constants.CDC_CAPTURE_INSTANCES_QUERY))
    tables = collections.defaultdict(list)

    for row in rows:
        if capture_instance_version_regex:
            row['regex_matched_group'] = \
                re.match(capture_instance_version_regex, row['capture_instance']).group(1)
        tables[row['source_object_id']].append(row)

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
        db_conn: sqlalchemy.engine.Connection, topic_name_template: str, table_whitelist_regex: str,
        table_blacklist_regex: str, snapshot_table_whitelist_regex: str, snapshot_table_blacklist_regex: str,
        capture_instance_version_strategy: str, capture_instance_version_regex: str) -> List[TrackedTable]:
    result = []

    latest_names = get_latest_capture_instance_names(db_conn, capture_instance_version_strategy,
                                                     capture_instance_version_regex)
    logger.debug('Latest capture instance names determined by "%s" strategy: %s', capture_instance_version_strategy,
                 sorted(latest_names))

    cdc_metadata_rows = db_conn.execute(text(constants.CDC_METADATA_QUERY),
                                        {'latest_capture_instance_names': latest_names})

    snapshot_table_whitelist_regex = snapshot_table_whitelist_regex and re.compile(snapshot_table_whitelist_regex)
    snapshot_table_blacklist_regex = snapshot_table_blacklist_regex and re.compile(snapshot_table_blacklist_regex)

    for cdc_metadata_row in cdc_metadata_rows:
        fq_table_name = f'{cdc_metadata_row["schema_name"]}.{cdc_metadata_row["table_name"]}'

        can_snapshot = False

        if table_whitelist_regex and not re.match(table_whitelist_regex, fq_table_name):
            logger.debug('Table %s excluded by whitelist', fq_table_name)
            continue

        if table_blacklist_regex and re.match(table_blacklist_regex, fq_table_name):
            logger.debug('Table %s excluded by blacklist', fq_table_name)
            continue

        if snapshot_table_whitelist_regex and re.match(snapshot_table_whitelist_regex, fq_table_name):
            logger.debug('Table %s WILL be snapshotted due to whitelisting', fq_table_name)
            can_snapshot = True

        if snapshot_table_blacklist_regex and re.match(snapshot_table_whitelist_regex, fq_table_name):
            logger.debug('Table %s will NOT be snapshotted due to blacklisting', fq_table_name)
            can_snapshot = False

        # ORDER BY in the SQL query ensures this means we're on to a new table:
        if cdc_metadata_row['change_table_ordinal'] == 1:
            topic_name = topic_name_template.format(**cdc_metadata_row)
            tracked_table = TrackedTable(
                db_conn, cdc_metadata_row['schema_name'], cdc_metadata_row['table_name'],
                cdc_metadata_row['capture_instance_name'], topic_name, can_snapshot)
            result.append(tracked_table)

        tracked_table.add_field(TrackedField(
            cdc_metadata_row['column_name'], cdc_metadata_row['sql_type_name'],
            cdc_metadata_row['change_table_ordinal'], cdc_metadata_row['primary_key_ordinal'],
            cdc_metadata_row['decimal_precision'], cdc_metadata_row['decimal_scale'],
            cdc_metadata_row['is_nullable'], cdc_metadata_row['is_identity']))

    return result
