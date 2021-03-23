import datetime

# Timing intervals

MIN_CDC_POLLING_INTERVAL = datetime.timedelta(seconds=3)
MAX_CDC_POLLING_INTERVAL = datetime.timedelta(seconds=10)
METRICS_REPORTING_INTERVAL = datetime.timedelta(seconds=20)
KAFKA_PRODUCER_FULL_FLUSH_INTERVAL = datetime.timedelta(seconds=60)
CHANGED_CAPTURE_INSTANCES_CHECK_INTERVAL = datetime.timedelta(seconds=60)
SQL_QUERY_TIMEOUT = datetime.timedelta(seconds=60)
SLOW_TABLE_PROGRESS_HEARTBEAT_INTERVAL = datetime.timedelta(minutes=3)
DB_CLOCK_SYNC_INTERVAL = datetime.timedelta(minutes=5)

WATERMARK_STABILITY_CHECK_DELAY_SECS = 10
KAFKA_REQUEST_TIMEOUT_SECS = 15
KAFKA_FULL_FLUSH_TIMEOUT_SECS = 30
KAFKA_CONFIG_RELOAD_DELAY_SECS = 2

# General

DB_ROW_BATCH_SIZE = 1000
MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT = '_row_hash'
DEFAULT_KEY_SCHEMA_COMPATIBILITY_LEVEL = 'FULL'
DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL = 'FORWARD'
UNIFIED_TOPIC_VALUE_SCHEMA_COMPATIBILITY_LEVEL = 'BACKWARD'
AVRO_SCHEMA_NAMESPACE = "cdc_to_kafka"
CDC_DB_SCHEMA_NAME = 'cdc'
UNRECOGNIZED_COLUMN_DEFAULT_NAME = 'UNKNOWN_COL'
VALIDATION_MAXIMUM_SAMPLE_SIZE_PER_TOPIC = 1_000_000

CHANGE_ROWS_KIND = "change_rows"
SNAPSHOT_ROWS_KIND = "snapshot_rows"

# CDC operation types; IDs 1-4 here match what SQL Server provides; ID 0 is of our own creation:

SNAPSHOT_OPERATION_ID = 0
SNAPSHOT_OPERATION_NAME = 'Snapshot'

DELETE_OPERATION_ID = 1
DELETE_OPERATION_NAME = 'Delete'

INSERT_OPERATION_ID = 2
INSERT_OPERATION_NAME = 'Insert'

PRE_UPDATE_OPERATION_ID = 3
PRE_UPDATE_OPERATION_NAME = 'PreUpdate'

POST_UPDATE_OPERATION_ID = 4
POST_UPDATE_OPERATION_NAME = 'PostUpdate'

CDC_OPERATION_ID_TO_NAME = {
    SNAPSHOT_OPERATION_ID: SNAPSHOT_OPERATION_NAME,
    DELETE_OPERATION_ID: DELETE_OPERATION_NAME,
    INSERT_OPERATION_ID: INSERT_OPERATION_NAME,
    PRE_UPDATE_OPERATION_ID: PRE_UPDATE_OPERATION_NAME,
    POST_UPDATE_OPERATION_ID: POST_UPDATE_OPERATION_NAME,
}

CDC_OPERATION_NAME_TO_ID = {
    SNAPSHOT_OPERATION_NAME: SNAPSHOT_OPERATION_ID,
    DELETE_OPERATION_NAME: DELETE_OPERATION_ID,
    INSERT_OPERATION_NAME: INSERT_OPERATION_ID,
    PRE_UPDATE_OPERATION_NAME: PRE_UPDATE_OPERATION_ID,
    POST_UPDATE_OPERATION_NAME: POST_UPDATE_OPERATION_ID,
}

# Metadata column names and positions

OPERATION_POS = 0
OPERATION_NAME = '__operation'

EVENT_TIME_POS = 1
EVENT_TIME_NAME = '__event_time'

LSN_POS = 2
LSN_NAME = '__log_lsn'

SEQVAL_POS = 3
SEQVAL_NAME = '__log_seqval'

UPDATED_FIELDS_POS = 4
UPDATED_FIELDS_NAME = '__updated_fields'

UNIFIED_TOPIC_MSG_SOURCE_TABLE_NAME = '__source_table'
UNIFIED_TOPIC_MSG_DATA_WRAPPER_NAME = '__change_data'

DB_LSN_COL_NAME = '__$start_lsn'
DB_SEQVAL_COL_NAME = '__$seqval'
DB_OPERATION_COL_NAME = '__$operation'

# Kafka message types

SINGLE_TABLE_CHANGE_MESSAGE = 'table-change'
UNIFIED_TOPIC_CHANGE_MESSAGE = 'unified-change'
SINGLE_TABLE_SNAPSHOT_MESSAGE = 'table-snapshot'
DELETION_CHANGE_TOMBSTONE_MESSAGE = 'deletion-tombstone'
CHANGE_PROGRESS_MESSAGE = 'change-progress'
SNAPSHOT_PROGRESS_MESSAGE = 'snapshot-progress'
HEARTBEAT_PROGRESS_MESSAGE = 'heartbeat-progress'
PROGRESS_DELETION_TOMBSTONE_MESSAGE = 'progress-deletion-tombstone'
METRIC_REPORTING_MESSAGE = 'metric-reporting'

ALL_KAFKA_MESSAGE_TYPES = (
    SINGLE_TABLE_CHANGE_MESSAGE, UNIFIED_TOPIC_CHANGE_MESSAGE, SINGLE_TABLE_SNAPSHOT_MESSAGE,
    DELETION_CHANGE_TOMBSTONE_MESSAGE, CHANGE_PROGRESS_MESSAGE, SNAPSHOT_PROGRESS_MESSAGE, HEARTBEAT_PROGRESS_MESSAGE,
    PROGRESS_DELETION_TOMBSTONE_MESSAGE, METRIC_REPORTING_MESSAGE)

# Unified topics schema

UNIFIED_TOPIC_SCHEMA_VERSION = '2'
UNIFIED_TOPIC_KEY_SCHEMA = {
    "name": f"{AVRO_SCHEMA_NAMESPACE}__unified_changes_v{UNIFIED_TOPIC_SCHEMA_VERSION}__key",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": LSN_NAME,
            "type": "string"
        }
    ]
}

UNIFIED_TOPIC_VALUE_SCHEMA = {
    "name": f"{AVRO_SCHEMA_NAMESPACE}__unified_changes_v{UNIFIED_TOPIC_SCHEMA_VERSION}__value",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": UNIFIED_TOPIC_MSG_SOURCE_TABLE_NAME,
            "type": "string"
        },
        {
            "name": UNIFIED_TOPIC_MSG_DATA_WRAPPER_NAME,
            "type": []  # this will be a union type of the record value schemas for all tables included in the topic
        }
    ]
}
