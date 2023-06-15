import datetime

# Timing intervals

MIN_CDC_POLLING_INTERVAL = datetime.timedelta(seconds=3)
MAX_CDC_POLLING_INTERVAL = datetime.timedelta(seconds=10)
METRICS_REPORTING_INTERVAL = datetime.timedelta(seconds=20)
KAFKA_PRODUCER_FULL_FLUSH_INTERVAL = datetime.timedelta(seconds=60)
CHANGED_CAPTURE_INSTANCES_CHECK_INTERVAL = datetime.timedelta(seconds=60)
SLOW_TABLE_PROGRESS_HEARTBEAT_INTERVAL = datetime.timedelta(minutes=3)
DB_CLOCK_SYNC_INTERVAL = datetime.timedelta(minutes=5)

SMALL_TABLE_THRESHOLD = 5_000_000
MAX_AGE_TO_PRESUME_ADDED_COL_IS_NULL_SECONDS = 3600

SQL_QUERY_TIMEOUT_SECONDS = 30
SQL_QUERY_INTER_RETRY_INTERVAL_SECONDS = 1
SQL_QUERY_RETRIES = 2

WATERMARK_STABILITY_CHECK_DELAY_SECS = 10
KAFKA_REQUEST_TIMEOUT_SECS = 15
KAFKA_FULL_FLUSH_TIMEOUT_SECS = 30
KAFKA_CONFIG_RELOAD_DELAY_SECS = 3

# General

MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT = '_row_hash'
DEFAULT_KEY_SCHEMA_COMPATIBILITY_LEVEL = 'FULL'
DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL = 'FORWARD'
AVRO_SCHEMA_NAMESPACE = "cdc_to_kafka"
CDC_DB_SCHEMA_NAME = 'cdc'
UNRECOGNIZED_COLUMN_DEFAULT_NAME = 'UNKNOWN_COL'
VALIDATION_MAXIMUM_SAMPLE_SIZE_PER_TOPIC = 1_000_000
SNAPSHOT_COMPLETION_SENTINEL = {'<< completed snapshot >>': '<< completed >>'}
SQL_STRING_TYPES = ('char', 'nchar', 'varchar', 'ntext', 'nvarchar', 'text')

CHANGE_ROWS_KIND = "change_rows"
SNAPSHOT_ROWS_KIND = "snapshot_rows"
ALL_PROGRESS_KINDS = "all_progress"

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

OPERATION_NAME = '__operation'
EVENT_TIME_NAME = '__event_time'
LSN_NAME = '__log_lsn'
SEQVAL_NAME = '__log_seqval'
UPDATED_FIELDS_NAME = '__updated_fields'

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
