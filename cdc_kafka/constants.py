import datetime
import json

import confluent_kafka.avro

from .tracked_tables import ChangeTableIndex

# General

DB_ROW_BATCH_SIZE = 1000
DB_TABLE_POLL_INTERVAL = datetime.timedelta(seconds=3)
STABLE_WATERMARK_CHECKS_INTERVAL_SECONDS = 1
PUBLISHED_COUNTS_LOGGING_INTERVAL = datetime.timedelta(seconds=60)
PROGRESS_COMMIT_INTERVAL = datetime.timedelta(seconds=3)
KAFKA_DELIVERY_SUCCESS_LOG_EVERY_NTH_MSG = 1000
BEGINNING_CHANGE_TABLE_INDEX = ChangeTableIndex(b'\x00' * 10, b'\x00' * 10, 0)
BEGINNING_DATETIME = datetime.datetime(2000, 1, 1)
MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT = 'row_hash'
AVRO_SCHEMA_NAMESPACE = 'cdc_to_kafka'

# Progress tracking schema

CHANGE_ROWS_PROGRESS_KIND = "change_rows"
SNAPSHOT_ROWS_PROGRESS_KIND = "snapshot_rows"
PROGRESS_MESSAGE_AVRO_KEY_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{AVRO_SCHEMA_NAMESPACE}_progress_key",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {"name": "topic_name", "type": "string"},
        {"name": "capture_instance_name", "type": "string"},
        {"name": "progress_kind", "type": {"type": "enum", "name": "progress_kind", "symbols": [
            CHANGE_ROWS_PROGRESS_KIND, SNAPSHOT_ROWS_PROGRESS_KIND
        ]}}
    ]
}))
PROGRESS_MESSAGE_AVRO_VALUE_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{AVRO_SCHEMA_NAMESPACE}_progress_value",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {"name": "last_published_change_table_lsn", "type": ["null", "bytes"]},
        {"name": "last_published_change_table_seqval", "type": ["null", "bytes"]},
        {"name": "last_published_change_table_operation", "type": ["null", "int"]},
        {"name": "last_published_incrementing_column_value", "type": ["null", "int"]},
    ]
}))

# CDC operation types

SNAPSHOT_OPERATION_ID = 0
SNAPSHOT_OPERATION_NAME = 'Snapshot'

CDC_OPERATION_ID_TO_NAME = {
    SNAPSHOT_OPERATION_ID: SNAPSHOT_OPERATION_NAME,
    1: "Delete",
    2: "Insert",
    3: "PreUpdate",
    4: "PostUpdate"
}
CDC_OPERATION_NAME_TO_ID = {v: k for k, v in CDC_OPERATION_ID_TO_NAME.items()}

# SQL queries

CDC_CAPTURE_INSTANCES_QUERY = '''
SELECT
    source_object_id
    , capture_instance
    , create_date
FROM cdc.change_tables
ORDER BY source_object_id
'''

CDC_METADATA_QUERY = '''
SELECT
    OBJECT_SCHEMA_NAME(ct.source_object_id) AS schema_name
    , OBJECT_NAME(ct.source_object_id) AS table_name
    , ct.capture_instance AS capture_instance_name
    , cc.column_ordinal AS change_table_ordinal
    , cc.column_name AS column_name
    , cc.column_type AS sql_type_name
    , ic.index_ordinal AS primary_key_ordinal
    , sc.precision AS decimal_precision
    , sc.scale AS decimal_scale
    , sc.is_nullable AS is_nullable
    , sc.is_identity AS is_identity
FROM
    cdc.change_tables AS ct
    INNER JOIN cdc.captured_columns AS cc ON (ct.object_id = cc.object_id)
    LEFT JOIN cdc.index_columns AS ic ON (cc.object_id = ic.object_id AND cc.column_id = ic.column_id)
    LEFT JOIN sys.columns AS sc ON (sc.object_id = ct.source_object_id AND sc.column_id = cc.column_id)
WHERE ct.capture_instance IN :latest_capture_instance_names
ORDER BY ct.object_id, cc.column_ordinal
'''

CDC_EARLIEST_LSN_QUERY = 'SELECT start_lsn FROM cdc.change_tables WHERE capture_instance = :capture_instance'

CHANGE_ROWS_QUERY_TEMPLATE = '''
SELECT TOP :number_to_get
    ct.__$start_lsn AS _cdc_start_lsn
    , ct.__$seqval AS _cdc_seqval
    , ct.__$operation AS _cdc_operation
    , ct.__$update_mask AS _cdc_update_mask
    , ltm.tran_end_time AS _cdc_tran_end_time
    , {fields}
FROM
    cdc.[{capture_instance_name}_CT] AS ct WITH (NOLOCK)
    LEFT JOIN cdc.lsn_time_mapping AS ltm WITH (NOLOCK) ON (ct.__$start_lsn = ltm.start_lsn)
WHERE
    __$operation != 3 AND
    (
        __$start_lsn > :lsn
        OR (__$start_lsn = :lsn AND __$seqval > :seqval)
        OR (__$start_lsn = :lsn AND __$seqval = :seqval AND __$operation > :operation)
    )
ORDER BY __$start_lsn, __$seqval, __$operation
'''

SNAPSHOT_ROWS_QUERY_TEMPLATE = '''
SELECT TOP :number_to_get
    0x00000000000000000000 AS _cdc_start_lsn
    , 0x00000000000000000000 AS _cdc_seqval
    , ''' + str(SNAPSHOT_OPERATION_ID) + ''' AS _cdc_operation
    , NULL AS _cdc_update_mask
    , GETDATE() AS _cdc_tran_end_time
    , {fields}
FROM
    [{schema_name}].[{table_name}] AS ct
WHERE [{incrementing_column}] < :increment_value
ORDER BY [{incrementing_column}] DESC
'''
