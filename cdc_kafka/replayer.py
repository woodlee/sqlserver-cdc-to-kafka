#!/usr/bin/env python3

"""
A tool for populating one or more tables that have been pre-created in a SQL Server DB based on data from Kafka
topics produced by CDC-to-Kafka. The script can replay a single topic to a single table (legacy mode) or replay
multiple topics to their corresponding tables in parallel.

One example use case would be to create a copy of an existing table (initially with a different name of course) on the
same DB from which a CDC-to-Kafka topic is produced, in order to rearrange indexes, types, etc. on the new copy of the
table. We're using this now to upgrade some INT columns to `BIGINT`s on a few tables that are nearing the 2^31 row
count. When the copy is ready, tables can be renamed so that applications begin using the new table.

EXAMPLE 1: Single topic replay
This assumes you have already created the Orders_copy table, with changes as desired, in the DB, and that you have
created the CdcTableCopier DB user there as well, presumably with limited permissions to work only with the
Orders_copy table. In this example we assume that 'OrderGuid' is a new column that exists on the new _copy table
only (perhaps populated by a default), and therefore we are not trying to sync that column since it doesn't exist
in the CDC feed/schema:

./replayer.py \
  --replay-topic 'dbo_Orders_cdc' \
  --kafka-bootstrap-servers 'localhost:9092' \
  --schema-registry-url 'http://localhost:8081' \
  --target-db-server 'localhost' \
  --target-db-user 'CdcTableCopier' \
  --target-db-password '*****' \
  --target-db-database 'MyDatabase' \
  --target-db-table-schema 'dbo' \
  --target-db-table-name 'Orders_copy' \
  --cols-to-not-sync 'OrderGuid'

EXAMPLE 2: Replaying multiple topics in parallel
This example replays three topics in parallel to their corresponding tables. The topic-to-table mapping should be
provided as a JSON object:

./replayer.py \
  --topic-to-table-map '{
    "dbo_Orders_cdc": {"schema": "dbo", "table": "Orders_copy"},
    "dbo_Customers_cdc": {"schema": "dbo", "table": "Customers_copy", "cols_to_not_sync": "CustomerGuid"},
    "dbo_Products_cdc": {"schema": "dbo", "table": "Products_copy"}
  }' \
  --kafka-bootstrap-servers 'localhost:9092' \
  --schema-registry-url 'http://localhost:8081' \
  --target-db-server 'localhost' \
  --target-db-user 'CdcTableCopier' \
  --target-db-password '*****' \
  --target-db-database 'MyDatabase'

Python package requirements:
    confluent-kafka[avro]==2.3.0
    ctds==1.14.0
    faster-fifo==1.4.5

"""

import argparse
import itertools
import json
import logging.config
import multiprocessing as mp
import os
import pprint
import queue as stdlib_queue
import socket
import time
from datetime import datetime, UTC
from multiprocessing.synchronize import Event as EventClass
from typing import Set, Any, List, Dict, Tuple, NamedTuple, Optional

import _tds
import ctds  # type: ignore[import-untyped]
from confluent_kafka import Consumer, KafkaError, TopicPartition, Message, OFFSET_BEGINNING
from confluent_kafka.admin import TopicMetadata
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from faster_fifo import Queue  # type: ignore[import-not-found]

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()


def parse_sql_default(col_default: Optional[str]) -> Any:
    if not col_default:
        return None

    val = col_default.strip()
    while val.startswith('(') and val.endswith(')'):
        val = val[1:-1].strip()

    if (not val) or val.upper() == 'NULL':
        return None

    # String literal: 'text' or N'text'
    if val.upper().startswith("N'") and val.endswith("'"):
        return val[2:-1].replace("''", "'")  # Unescape doubled quotes
    if val.startswith("'") and val.endswith("'"):
        return val[1:-1].replace("''", "'")

    # Numeric literal
    try:
        if '.' in val:
            return float(val)
        return int(val)
    except ValueError:
        pass

    # Anything else (functions like getdate(), newid(), complex expressions)
    # Return None and let SQL Server handle it or accept NULL
    return None


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        __name__: {
            'handlers': ['console'],
            'level': log_level,
            'propagate': True,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': log_level,
            'formatter': 'simple',
        },
    },
    'formatters': {
        'simple': {
            'format': '%(asctime)s %(levelname)-8s (%(processName)s) [%(name)s:%(lineno)s] %(message)s',
        },
    },
})

logger = logging.getLogger(__name__)


CDC_OPERATION_NAME_TO_ID = {
    'Snapshot': 0,
    'Delete': 1,
    'Insert': 2,
    'PreUpdate': 3,
    'PostUpdate': 4,
}


class LsnPosition(NamedTuple):
    """Represents an LSN position for comparison, using __log_lsn, __log_seqval, and operation.

    Comparison follows SQL Server CDC ordering: lsn first, then seqval, then operation.
    """
    lsn: bytes
    seqval: bytes
    operation: int

    @staticmethod
    def from_message(msg_val: Dict[str, Any]) -> 'LsnPosition':
        """Create an LsnPosition from a deserialized message value."""
        lsn_hex = msg_val.get('__log_lsn', '0x' + '00' * 10)
        seqval_hex = msg_val.get('__log_seqval', '0x' + '00' * 10)
        operation_name = msg_val.get('__operation', 'Snapshot')
        return LsnPosition(
            lsn=bytes.fromhex(lsn_hex[2:]),  # Strip '0x' prefix
            seqval=bytes.fromhex(seqval_hex[2:]),
            operation=CDC_OPERATION_NAME_TO_ID.get(operation_name, 0)
        )

    def __repr__(self) -> str:
        return f'LSN(0x{self.lsn.hex()}:{self.seqval.hex()}:{self.operation})'


LOWEST_LSN_POSITION = LsnPosition(b'\x00' * 10, b'\x00' * 10, 0)
HIGHEST_LSN_POSITION = LsnPosition(b'\xff' * 10, b'\xff' * 10, 4)


class Progress(NamedTuple):
    source_topic_name: str
    source_topic_partition: int
    target_table_object_id: int
    target_table_schema_name: str
    target_table_name: str
    last_handled_message_offset: int
    last_handled_message_timestamp: datetime
    commit_time: datetime
    replayer_progress_namespace: str
    replayer_process_id: str


class ReplayConfig(NamedTuple):
    replay_topic: str
    target_db_table_schema: str
    target_db_table_name: str
    cols_to_not_sync: str
    primary_key_fields_override: str


def get_progress(opts: argparse.Namespace, conn: ctds.Connection) -> List[Progress]:
    target_table_fq_name: str = \
        f'[{opts.target_db_table_schema.strip()}].[{opts.target_db_table_name.strip()}]'
    progress_table_fq_name: str = \
        f'[{opts.progress_tracking_table_schema.strip()}].[{opts.progress_tracking_table_name.strip()}]'

    with conn.cursor() as cursor:
        cursor.execute(f'''
IF OBJECT_ID(:0) IS NULL 
BEGIN       
    CREATE TABLE {progress_table_fq_name} (
        [source_topic_name]               VARCHAR(255) NOT NULL,
        [source_topic_partition]          INT          NOT NULL,
        [target_table_object_id]          INT          NOT NULL,
        [target_table_schema_name]        SYSNAME      NOT NULL,
        [target_table_name]               SYSNAME      NOT NULL,
        [last_handled_message_offset]     BIGINT,
        [last_handled_message_timestamp]  DATETIME2(3),
        [commit_time]                     DATETIME2(3),
        [replayer_progress_namespace]     VARCHAR(255) NOT NULL,
        [replayer_process_id]             VARCHAR(255) NOT NULL,
        CONSTRAINT [PK_{opts.progress_tracking_table_name.strip()}] PRIMARY KEY CLUSTERED (
            [source_topic_name], [target_table_object_id], [replayer_progress_namespace], [source_topic_partition]
        )
    )
END
        ''', (progress_table_fq_name,))

        cursor.execute(f'''        
SELECT [source_topic_name]
    , [source_topic_partition]
    , [target_table_object_id]
    , [target_table_schema_name]
    , [target_table_name]
    , [last_handled_message_offset]
    , [last_handled_message_timestamp]
    , [commit_time]
    , [replayer_progress_namespace]
    , [replayer_process_id]
FROM {progress_table_fq_name}
WHERE [source_topic_name] = :0 
    AND [target_table_object_id] = OBJECT_ID(:1)
    AND [replayer_progress_namespace] = :2
        ''', (opts.replay_topic, target_table_fq_name, opts.progress_tracking_namespace))

        return [Progress(*row) for row in cursor.fetchall()]


def commit_progress(opts: argparse.Namespace, last_consume_by_partition: Dict[int, Tuple[int, int]],
                    proc_id: str, db_conn: ctds.Connection) -> None:
    progress_table_fq_name: str = \
        f'[{opts.progress_tracking_table_schema.strip()}].[{opts.progress_tracking_table_name.strip()}]'

    with db_conn.cursor() as cursor:
        for partition, (offset, timestamp) in last_consume_by_partition.items():
            cursor.execute(f'''        
MERGE {progress_table_fq_name} AS pt
USING (SELECT 
    :0 AS [source_topic_name]
    , :1 AS [source_topic_partition]
    , OBJECT_ID(:2 + '.' + :3) AS [target_table_object_id]
    , :2 AS [target_table_schema_name]
    , :3 AS [target_table_name]
    , :4 AS [last_handled_message_offset]
    , :5 AS [last_handled_message_timestamp]
    , GETDATE() AS [commit_time]
    , :6 AS [replayer_progress_namespace]
    , :7 AS [replayer_process_id]
) AS row ON (pt.[source_topic_name] = row.[source_topic_name]
    AND pt.[target_table_object_id] = row.[target_table_object_id]
    AND pt.[replayer_progress_namespace] = row.[replayer_progress_namespace]
    AND pt.[source_topic_partition] = row.[source_topic_partition]
)
WHEN MATCHED THEN UPDATE SET
    [last_handled_message_offset] = row.[last_handled_message_offset]
    , [last_handled_message_timestamp] = row.[last_handled_message_timestamp]
    , [commit_time] = row.[commit_time]
    , [replayer_process_id] = row.[replayer_process_id]
WHEN NOT MATCHED THEN INSERT (
    [source_topic_name]
    , [source_topic_partition]
    , [target_table_object_id]
    , [target_table_schema_name]
    , [target_table_name]
    , [last_handled_message_offset]
    , [last_handled_message_timestamp]
    , [commit_time]
    , [replayer_progress_namespace]
    , [replayer_process_id]
)
VALUES (
    row.[source_topic_name]
    , row.[source_topic_partition]
    , row.[target_table_object_id]
    , row.[target_table_schema_name]
    , row.[target_table_name]
    , row.[last_handled_message_offset]
    , row.[last_handled_message_timestamp]
    , row.[commit_time]
    , row.[replayer_progress_namespace]
    , row.[replayer_process_id]
);
            ''', (opts.replay_topic, partition, opts.target_db_table_schema, opts.target_db_table_name, offset,
                  datetime.fromtimestamp(timestamp / 1000, UTC).replace(tzinfo=None),
                  opts.progress_tracking_namespace, proc_id))
        db_conn.commit()


def commit_cb(err: KafkaError, tps: List[TopicPartition]) -> None:
    if err is not None:
        logger.error(f'Error committing offsets: {err}')
    else:
        logger.debug(f'Offsets committed for {tps}')


def format_coordinates(msg: Message) -> str:
    return f'{msg.topic()}:{msg.partition()}@{msg.offset()}, ' \
           f'time {datetime.fromtimestamp(msg.timestamp()[1] / 1000, UTC)}'


def get_latest_lsn_from_all_changes_topic(opts: argparse.Namespace) -> Tuple[LsnPosition, int]:
    """Get the LSN and offset of the most recent message in the all-changes topic.

    Returns a tuple of (LsnPosition, offset) for the latest message.
    """
    schema_registry_client = SchemaRegistryClient({'url': opts.schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer_conf = {
        'bootstrap.servers': opts.kafka_bootstrap_servers,
        'group.id': f'replayer-lsn-lookup-{int(datetime.now().timestamp())}',
        'enable.auto.offset.store': False,
        'enable.auto.commit': False,
        'auto.offset.reset': 'latest',
        **opts.extra_kafka_consumer_config
    }
    consumer = Consumer(consumer_conf)

    try:
        # Get topic metadata
        topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
            topic=opts.all_changes_topic).topics  # type: ignore[call-arg]
        if topics_meta is None:
            raise Exception(f'No metadata found for topic {opts.all_changes_topic}')

        partitions = list((topics_meta[opts.all_changes_topic].partitions or {}).keys())
        if not partitions:
            raise Exception(f'No partitions found for topic {opts.all_changes_topic}')

        # For each partition, get the high watermark and read the last message
        latest_lsn = LOWEST_LSN_POSITION
        latest_offset = -1

        for partition_id in partitions:
            tp = TopicPartition(opts.all_changes_topic, partition_id)
            low, high = consumer.get_watermark_offsets(tp)

            if high <= low:
                logger.debug(f'Partition {partition_id} is empty (low={low}, high={high})')
                continue

            # Seek to the last message
            tp.offset = high - 2
            consumer.assign([tp])

            msg = consumer.poll(timeout=10.0)
            if msg is None:
                logger.warning(f'Could not read last message from partition {partition_id}')
                continue

            err = msg.error()
            if err:
                logger.warning(f'Error reading from partition {partition_id}: {err}')
                continue

            raw_val = msg.value()
            if raw_val is None:
                continue

            msg_val = avro_deserializer(raw_val, SerializationContext(opts.all_changes_topic, MessageField.VALUE))
            if msg_val is None:
                continue

            msg_lsn = LsnPosition.from_message(msg_val)
            if msg_lsn > latest_lsn:
                latest_lsn = msg_lsn
                latest_offset = msg.offset()

        if latest_offset < 0:
            raise Exception(f'Could not find any messages in topic {opts.all_changes_topic}')

        logger.info(f'Found latest LSN in all-changes topic: {latest_lsn} at offset {latest_offset}')
        return latest_lsn, latest_offset

    finally:
        consumer.close()


def get_all_changes_topic_progress(opts: argparse.Namespace, db_conn: ctds.Connection) -> Optional[Progress]:
    """Get the progress record for the all-changes topic, if it exists."""
    progress_table_fq_name = f'[{opts.progress_tracking_table_schema}].[{opts.progress_tracking_table_name}]'

    with db_conn.cursor() as cursor:
        cursor.execute(f'''
SELECT [source_topic_name]
    , [source_topic_partition]
    , [target_table_object_id]
    , [target_table_schema_name]
    , [target_table_name]
    , [last_handled_message_offset]
    , [last_handled_message_timestamp]
    , [commit_time]
    , [replayer_progress_namespace]
    , [replayer_process_id]
FROM {progress_table_fq_name}
WHERE [source_topic_name] = :0
    AND [replayer_progress_namespace] = :1
        ''', (opts.all_changes_topic, opts.progress_tracking_namespace))

        row = cursor.fetchone()
        if row:
            return Progress(*row)
        return None


def commit_all_changes_topic_progress(opts: argparse.Namespace, offset: int, timestamp: int,
                                      proc_id: str, db_conn: ctds.Connection) -> None:
    """Commit progress for the all-changes topic."""
    progress_table_fq_name = f'[{opts.progress_tracking_table_schema}].[{opts.progress_tracking_table_name}]'

    with db_conn.cursor() as cursor:
        cursor.execute(f'''
MERGE {progress_table_fq_name} AS pt
USING (SELECT
    :0 AS [source_topic_name]
    , 0 AS [source_topic_partition]
    , 0 AS [target_table_object_id]
    , '' AS [target_table_schema_name]
    , '' AS [target_table_name]
    , :1 AS [last_handled_message_offset]
    , :2 AS [last_handled_message_timestamp]
    , GETDATE() AS [commit_time]
    , :3 AS [replayer_progress_namespace]
    , :4 AS [replayer_process_id]
) AS row ON (pt.[source_topic_name] = row.[source_topic_name]
    AND pt.[replayer_progress_namespace] = row.[replayer_progress_namespace]
    AND pt.[source_topic_partition] = row.[source_topic_partition]
)
WHEN MATCHED THEN UPDATE SET
    [last_handled_message_offset] = row.[last_handled_message_offset]
    , [last_handled_message_timestamp] = row.[last_handled_message_timestamp]
    , [commit_time] = row.[commit_time]
    , [replayer_process_id] = row.[replayer_process_id]
WHEN NOT MATCHED THEN INSERT (
    [source_topic_name]
    , [source_topic_partition]
    , [target_table_object_id]
    , [target_table_schema_name]
    , [target_table_name]
    , [last_handled_message_offset]
    , [last_handled_message_timestamp]
    , [commit_time]
    , [replayer_progress_namespace]
    , [replayer_process_id]
)
VALUES (
    row.[source_topic_name]
    , row.[source_topic_partition]
    , row.[target_table_object_id]
    , row.[target_table_schema_name]
    , row.[target_table_name]
    , row.[last_handled_message_offset]
    , row.[last_handled_message_timestamp]
    , row.[commit_time]
    , row.[replayer_progress_namespace]
    , row.[replayer_process_id]
);
        ''', (opts.all_changes_topic, offset,
              datetime.fromtimestamp(timestamp / 1000, UTC).replace(tzinfo=None),
              opts.progress_tracking_namespace, proc_id))
        db_conn.commit()


def replay_worker(config: ReplayConfig, opts: argparse.Namespace, stop_event: EventClass, queue: Queue,
                  proc_id: str, cutoff_lsn: Optional[LsnPosition] = None) -> None:
    """Worker process that replays a single topic to a single table.

    If cutoff_lsn is provided (backfill mode), the worker stops when it sees a message
    with an LSN exceeding the cutoff.
    """
    logger.info(f"Starting replay worker for topic '{config.replay_topic}' -> "
                f"[{config.target_db_table_schema}].[{config.target_db_table_name}]"
                f"{f' (cutoff: {cutoff_lsn})' if cutoff_lsn else ''}")

    delete_cnt: int = 0
    upsert_cnt: int = 0
    poll_time_acc: float = 0.0
    sql_time_acc: float = 0.0
    queue_get_wait: float = 0.0
    last_commit_time: datetime = datetime.now()
    last_consume_by_partition: Dict[int, Tuple[int, int]] = {}
    queued_deletes: Set[Any] = set()
    queued_upserts: Dict[Any, Tuple[str, List[Any]]] = {}
    fq_target_table_name: str = f'[{config.target_db_table_schema.strip()}].[{config.target_db_table_name.strip()}]'
    temp_table_base_name: str = f'#replayer_{config.target_db_table_schema.strip()}_{config.target_db_table_name.strip()}'
    delete_temp_table_name: str = temp_table_base_name + '_delete'
    merge_temp_table_name: str = temp_table_base_name + '_merge'
    cols_to_not_sync: set[str] = set([c.strip().lower() for c in config.cols_to_not_sync.split(',')])
    cols_to_not_sync.discard('')
    proc_start_time: float = time.perf_counter()

    # Initialize Avro deserializer for this worker
    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient({'url': opts.schema_registry_url})
    avro_deserializer: AvroDeserializer = AvroDeserializer(schema_registry_client)

    try:
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                          database=opts.target_db_database, timeout=15, login_timeout=15) as db_conn:
            with db_conn.cursor() as cursor:
                # Create a modified opts object for this specific topic/table
                worker_opts = argparse.Namespace(**vars(opts))
                worker_opts.replay_topic = config.replay_topic
                worker_opts.target_db_table_schema = config.target_db_table_schema
                worker_opts.target_db_table_name = config.target_db_table_name
                worker_opts.cols_to_not_sync = config.cols_to_not_sync
                worker_opts.primary_key_fields_override = config.primary_key_fields_override

                primary_key_field_names: List[str]
                if config.primary_key_fields_override.strip():
                    primary_key_field_names = [x.strip() for x in config.primary_key_fields_override.split(',')]
                else:
                    cursor.execute(f'''
SELECT [COLUMN_NAME]
FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE]
WHERE OBJECTPROPERTY(OBJECT_ID([CONSTRAINT_SCHEMA] + '.' + QUOTENAME([CONSTRAINT_NAME])), 'IsPrimaryKey') = 1
AND [TABLE_SCHEMA] = :0
AND [TABLE_NAME] = :1
ORDER BY [ORDINAL_POSITION]
                    ''', (config.target_db_table_schema, config.target_db_table_name))
                    primary_key_field_names = [r[0] for r in cursor.fetchall()]

                field_names: List[str] = []
                datetime_field_names: set[str] = set()
                varchar_field_names: set[str] = set()
                nvarchar_field_names: set[str] = set()
                delete_temp_table_col_specs: List[str] = []
                column_defaults: Dict[str, Any] = {}

                cursor.execute('''
SELECT name
FROM sys.computed_columns
WHERE OBJECT_SCHEMA_NAME(object_id) = :0
    AND OBJECT_NAME(object_id) = :1                
                ''', (config.target_db_table_schema, config.target_db_table_name))
                computed_cols: set[str] = {r[0].lower() for r in cursor.fetchall()}

                cursor.execute('''
SELECT [COLUMN_NAME]
    , [DATA_TYPE]
    , COALESCE([CHARACTER_MAXIMUM_LENGTH], [DATETIME_PRECISION]) AS [PRECISION_SPEC]
    , [IS_NULLABLE]
    , [COLUMN_DEFAULT]
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE [TABLE_SCHEMA] = :0
    AND [TABLE_NAME] = :1
ORDER BY [ORDINAL_POSITION]
                ''', (config.target_db_table_schema, config.target_db_table_name))
                for col_name, col_type, col_precision, col_is_nullable, col_default in cursor.fetchall():
                    if col_name.lower() in cols_to_not_sync or col_name.lower() in computed_cols:
                        continue
                    field_names.append(col_name)
                    if col_type.lower().startswith('datetime'):  # TODO: more cases than "datetime"?
                        datetime_field_names.add(col_name)
                    if col_type.lower() in ('char', 'varchar', 'text'):
                        varchar_field_names.add(col_name)
                    if col_type.lower() in ('nchar', 'nvarchar', 'ntext'):
                        nvarchar_field_names.add(col_name)
                    column_defaults[col_name] = parse_sql_default(col_default)
                    if col_name in primary_key_field_names:
                        precision: str = f'({col_precision})' if col_precision is not None else ''
                        nullability: str = '' if col_is_nullable else 'NOT NULL'
                        delete_temp_table_col_specs.append(f'[{col_name}] {col_type}{precision} {nullability}')

                cursor.execute(f'DROP TABLE IF EXISTS {merge_temp_table_name};')
                # Yep, this looks weird--it's a hack to prevent SQL Server from copying over the IDENTITY property
                # of any columns that have it whenever it creates the temp table. https://stackoverflow.com/a/57509258
                cursor.execute(f'SELECT TOP 0 * INTO {merge_temp_table_name} FROM {fq_target_table_name} '
                               f'UNION ALL SELECT * FROM {fq_target_table_name} WHERE 1 <> 1;')
                for c in itertools.chain(cols_to_not_sync, computed_cols):
                    cursor.execute(f'ALTER TABLE {merge_temp_table_name} DROP COLUMN [{c}];')

                cursor.execute(f'DROP TABLE IF EXISTS {delete_temp_table_name};')
                cursor.execute(f'''
CREATE TABLE {delete_temp_table_name} (
    {",".join(delete_temp_table_col_specs)},
    CONSTRAINT [PK_{delete_temp_table_name}]
    PRIMARY KEY ([{"], [".join(primary_key_field_names)}])
);
                ''')

                delete_join_predicates: str = ' AND '.join([f'tgt.[{c}] = dtt.[{c}]' for c in primary_key_field_names])
                delete_stmt: str = f'''
DELETE tgt
FROM {fq_target_table_name} AS tgt
INNER JOIN {delete_temp_table_name} AS dtt ON ({delete_join_predicates});

TRUNCATE TABLE {delete_temp_table_name};
                '''

                cursor.execute('SELECT TOP 1 [name] FROM sys.columns WHERE object_id = OBJECT_ID(:0) AND is_identity = 1',
                               (fq_target_table_name,))
                rows = cursor.fetchall()
                identity_col_name: Optional[str] = rows and rows[0][0] or None
                set_identity_insert_if_needed = f'SET IDENTITY_INSERT {fq_target_table_name} ON; ' \
                    if identity_col_name else ''

                merge_match_predicates: str = ' AND '.join([f'tgt.[{c}] = src.[{c}]' for c in primary_key_field_names])

                # This is a real edge case, but if all the table cols are in the PK, then SQL always models an
                # update as an insert+delete in CDC data, so the WHEN MATCHED THEN UPDATE SET would wind up empty
                # which is syntactically invalid:
                merge_stmt: str
                if set(field_names) == set(primary_key_field_names):
                    merge_stmt = f'''
{set_identity_insert_if_needed}
MERGE {fq_target_table_name} AS tgt
USING {merge_temp_table_name} AS src
    ON ({merge_match_predicates})
WHEN NOT MATCHED THEN
    INSERT ([{'], ['.join(field_names)}]) VALUES (src.[{'], src.['.join(field_names)}]);

TRUNCATE TABLE {merge_temp_table_name};
                    '''
                else:
                    merge_stmt = f'''
{set_identity_insert_if_needed}
MERGE {fq_target_table_name} AS tgt
USING {merge_temp_table_name} AS src
    ON ({merge_match_predicates})
WHEN MATCHED THEN
    UPDATE SET {", ".join([f'[{x}] = src.[{x}]' for x in field_names if x not in primary_key_field_names and x != identity_col_name])}
WHEN NOT MATCHED THEN
    INSERT ([{'], ['.join(field_names)}]) VALUES (src.[{'], src.['.join(field_names)}]);

TRUNCATE TABLE {merge_temp_table_name};
                    '''
                # Truncate target table if requested and no prior progress exists
                existing_progress = get_progress(worker_opts, db_conn)
                if opts.truncate_existing_data and not existing_progress:
                    logger.info(f"Worker {config.replay_topic}: Truncating target table {fq_target_table_name} "
                               f"(no prior progress exists)")
                    cursor.execute(f'TRUNCATE TABLE {fq_target_table_name};')
                    db_conn.commit()
                elif opts.truncate_existing_data and existing_progress:
                    logger.info(f"Worker {config.replay_topic}: Skipping truncation of {fq_target_table_name} "
                               f"(prior progress exists)")

            proc_start_time = time.perf_counter()
            logger.debug(f"Worker for '{config.replay_topic}': Listening for consumed messages.")

            while True:
                queue_get_wait_start = time.perf_counter()
                try:
                    topic, msg_partition, msg_offset, msg_timestamp, raw_key, raw_val = queue.get(timeout=0.1)
                except stdlib_queue.Empty:
                    topic, msg_partition, msg_offset, msg_timestamp, raw_key, raw_val = None, None, None, None, None, None
                queue_get_wait += time.perf_counter() - queue_get_wait_start

                # Check for EOF sentinel (topic is set but partition is None when consumer reached end of topic)
                # Note: when queue.get times out, topic is also None, so we check topic is not None
                reached_eof = topic is not None and msg_partition is None
                if reached_eof:
                    logger.info(f"Worker {config.replay_topic}: received EOF sentinel (topic has been fully consumed). "
                               f"Will flush and stop.")

                # Deserialize Avro messages in parallel workers
                msg_key: Optional[Dict[str, Any]] = None
                msg_val: Optional[Dict[str, Any]] = None
                if raw_key is not None:
                    # noinspection PyNoneFunctionAssignment
                    msg_key = avro_deserializer(raw_key, SerializationContext(topic, MessageField.KEY))  # type: ignore[func-returns-value, arg-type]
                if raw_val is not None:
                    # noinspection PyNoneFunctionAssignment
                    msg_val = avro_deserializer(raw_val, SerializationContext(topic, MessageField.VALUE))  # type: ignore[func-returns-value, arg-type]

                # Check cutoff LSN in backfill mode
                reached_cutoff = False
                if cutoff_lsn is not None and msg_val is not None:
                    msg_lsn = LsnPosition.from_message(msg_val)
                    if msg_lsn > cutoff_lsn:
                        logger.info(f"Worker {config.replay_topic}: reached cutoff LSN at offset {msg_offset} "
                                   f"(msg LSN {msg_lsn} > cutoff {cutoff_lsn}). Will flush and stop.")
                        reached_cutoff = True
                        # Clear the current message so we don't process it, but continue to flush
                        msg_key = None
                        msg_val = None

                if reached_cutoff or reached_eof or len(queued_deletes) >= opts.delete_batch_size or \
                        len(queued_upserts) >= opts.upsert_batch_size or \
                        (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds:
                    with db_conn.cursor() as cursor:
                        if queued_deletes:
                            start_time = time.perf_counter()
                            #logger.info(queued_deletes)
                            db_conn.bulk_insert(delete_temp_table_name, list(queued_deletes))
                            sql_time_acc += time.perf_counter() - start_time
                            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            logger.info('Worker %s: Inserted to temp table for delete: %s items in %s ms',
                                        config.replay_topic, len(queued_deletes), elapsed_ms)

                            start_time = time.perf_counter()
                            cursor.execute(delete_stmt)
                            sql_time_acc += time.perf_counter() - start_time
                            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            logger.info('Worker %s: Deleted %s items in %s ms',
                                        config.replay_topic, len(queued_deletes), elapsed_ms)

                        if queued_upserts:
                            inserts: List[Any] = []
                            merges: List[Any] = []
                            for _, (op, val) in queued_upserts.items():
                                # CTDS unfortunately completely ignores values for target-table IDENTITY cols when doing
                                # a bulk_insert, so in that case we have to fall back to the slower MERGE mechanism:
                                if (not identity_col_name) and op == 'Insert':  # in ('Snapshot', 'Insert'): -- Snapshots can hit PK collisions! :(
                                    inserts.append(val)
                                else:
                                    merges.append(val)

                            # Inserts CAN be treated as merges, and it's probably more efficient to do so if there
                            # are relatively few of them, to cut down on DB round-trips:
                            if inserts and merges and len(inserts) < 100 or \
                                    len(inserts) / (len(inserts) + len(merges)) < 0.1:
                                logger.debug('Worker %s: Rolling %s inserts into %s merges',
                                             config.replay_topic, len(inserts), len(merges))
                                merges += inserts
                                inserts = []

                            if inserts:
                                start_time = time.perf_counter()
                                #logger.info(inserts)
                                db_conn.bulk_insert(fq_target_table_name, inserts)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Worker %s: Inserted directly to target table: %s items in %s ms',
                                            config.replay_topic, len(inserts), elapsed_ms)

                            if merges:
                                start_time = time.perf_counter()
                                try:
                                    db_conn.bulk_insert(merge_temp_table_name, merges)
                                except _tds.DatabaseError as e:
                                    pprint.pprint(merges)
                                    raise e
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Worker %s: Inserted to temp table for merge: %s items in %s ms',
                                            config.replay_topic, len(merges), elapsed_ms)

                                start_time = time.perf_counter()
                                #logger.info(merge_stmt)
                                cursor.execute(merge_stmt)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Worker %s: Merged %s items in %s ms',
                                            config.replay_topic, len(merges), elapsed_ms)

                        if queued_upserts or queued_deletes:
                            queued_upserts.clear()
                            queued_deletes.clear()
                            commit_progress(worker_opts, last_consume_by_partition, proc_id, db_conn)
                            last_commit_time = datetime.now()

                    if reached_cutoff or reached_eof:
                        logger.info(f"Worker {config.replay_topic}: flushed remaining work, exiting.")
                        break

                    if stop_event.is_set() and queue.empty():
                        break

                if msg_key is None:
                    continue

                key_val: Tuple[Any, ...] = tuple((msg_key[x] for x in primary_key_field_names))

                if msg_val is None or msg_val['__operation'] == 'Delete':
                    queued_deletes.add(key_val)
                    queued_upserts.pop(key_val, None)
                    delete_cnt += 1
                else:
                    vals: List[Any] = []
                    for f in field_names:
                        if f not in msg_val:
                            vals.append(column_defaults[f])
                        elif msg_val[f] is None:
                            vals.append(None)
                        elif f in datetime_field_names:
                            dt: datetime = datetime.fromisoformat(msg_val[f])
                            if dt.year < 1753:
                            # FML--something in either CTDS or FreeTDS gets weird when trying to BCP anything earlier
                            # so we're just going to standardize the cutoff for anything before this (which is likely
                            # bad data anyway):
                                dt = datetime(1753, 1, 1, 0, 0, 0)
                            vals.append(dt)
                        elif f in varchar_field_names:
                            # The below assumes your DB uses SQL_Latin1_General_CP1_CI_AS collation; if not, you may
                            # need to change 'cp1252' to something else:
                            vals.append(ctds.SqlVarChar(msg_val[f].encode('cp1252')))
                        elif f in nvarchar_field_names:
                            # See https://zillow.github.io/ctds/bulk_insert.html#text-columns
                            vals.append(ctds.SqlVarChar(msg_val[f].encode('utf-16le')))
                        else:
                            vals.append(msg_val[f])
                    queued_upserts[key_val] = (msg_val['__operation'], vals)
                    # Don't do this: deletes run first in the processing loop. Let that happen--if you don't, there
                    # is a chance that a delete-followed-by-insert happens in one batch and if you don't let that
                    # delete happen first, the insert will encounter a PK constraint violation:
                    #
                    # queued_deletes.discard(key_val)
                    upsert_cnt += 1

                if (len(queued_deletes) + len(queued_upserts)) % 5_000 == 0:
                    logger.debug('Worker %s: Currently have %s queued deletes and %s queued upserts. '
                                 'Time waiting on queue = %s. Apx. queue depth %s',
                                 config.replay_topic, len(queued_deletes), len(queued_upserts),
                                 queue_get_wait, queue.qsize())
                    queue_get_wait = 0

                last_consume_by_partition[msg_partition] = (msg_offset, msg_timestamp)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(f"Worker for '{config.replay_topic}' encountered error: {e}")
    finally:
        stop_event.set()
        logger.info(f"Worker for '{config.replay_topic}': Processed {delete_cnt} deletes, {upsert_cnt} upserts.")
        overall_time = time.perf_counter() - proc_start_time
        logger.info(f"Worker for '{config.replay_topic}' total times:\n"
                    f"Kafka poll: {poll_time_acc:.2f}s\nSQL execution: {sql_time_acc:.2f}s\n"
                    f"Overall: {overall_time:.2f}s")


def main() -> None:
    p = argparse.ArgumentParser()

    # Config for data source
    p.add_argument('--replay-topic',
                   default=os.environ.get('REPLAY_TOPIC'),
                   help='Single topic to replay (for backward compatibility)')
    p.add_argument('--topic-to-table-map',
                   default=os.environ.get('TOPIC_TO_TABLE_MAP'),
                   help='JSON mapping of topics to tables, e.g. {"topic1": {"schema": "dbo", "table": "Table1"}, ...}')
    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))
    p.add_argument('--extra-kafka-consumer-config',
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG', {}), type=json.loads)

    # Config for data target / progress tracking
    p.add_argument('--target-db-server',
                   default=os.environ.get('TARGET_DB_SERVER'))
    p.add_argument('--target-db-user',
                   default=os.environ.get('TARGET_DB_USER'))
    p.add_argument('--target-db-password',
                   default=os.environ.get('TARGET_DB_PASSWORD'))
    p.add_argument('--target-db-database',
                   default=os.environ.get('TARGET_DB_DATABASE'))
    p.add_argument('--target-db-table-schema',
                   default=os.environ.get('TARGET_DB_TABLE_SCHEMA'),
                   help='Single table schema (for backward compatibility)')
    p.add_argument('--target-db-table-name',
                   default=os.environ.get('TARGET_DB_TABLE_NAME'),
                   help='Single table name (for backward compatibility)')
    p.add_argument('--cols-to-not-sync',
                   default=os.environ.get('COLS_TO_NOT_SYNC', ''))
    p.add_argument('--primary-key-fields-override',
                   default=os.environ.get('PRIMARY_KEY_FIELDS_OVERRIDE', ''))
    p.add_argument('--progress-tracking-namespace',
                   default=os.environ.get('PROGRESS_TRACKING_NAMESPACE', 'default'))
    p.add_argument('--progress-tracking-table-schema',
                   default=os.environ.get('PROGRESS_TRACKING_TABLE_SCHEMA', 'dbo'))
    p.add_argument('--progress-tracking-table-name',
                   default=os.environ.get('PROGRESS_TRACKING_TABLE_NAME', 'CdcKafkaReplayerProgress'))

    # Config for process behavior and tuning
    p.add_argument('--delete-batch-size', type=int,
                   default=os.environ.get('DELETE_BATCH_SIZE', 2000))
    p.add_argument('--upsert-batch-size', type=int,
                   default=os.environ.get('UPSERT_BATCH_SIZE', 5000))
    p.add_argument('--max-commit-latency-seconds', type=int,
                   default=os.environ.get('MAX_COMMIT_LATENCY_SECONDS', 10))
    p.add_argument('--consumed-messages-limit', type=int,
                   default=os.environ.get('CONSUMED_MESSAGES_LIMIT', 0))
    p.add_argument('--truncate-existing-data', action='store_true',
                   default=os.environ.get('TRUNCATE_EXISTING_DATA', '').lower() in ('true', '1', 'yes'),
                   help='Truncate target table data if no prior progress exists')

    # Mode selection for backfill vs follow
    p.add_argument('--mode',
                   default=os.environ.get('REPLAYER_MODE', 'backfill'),
                   choices=['backfill', 'follow'],
                   help='Operation mode: "backfill" replays single-table topics in parallel up to a cutoff LSN, '
                        '"follow" reads the all-changes topic in order to maintain FK constraints')
    p.add_argument('--all-changes-topic',
                   default=os.environ.get('ALL_CHANGES_TOPIC'),
                   help='Name of the unified all-changes topic (e.g., "ssy_all_cdc_changes") containing messages '
                        'from all tables in LSN order')

    opts, _ = p.parse_known_args()

    # Validate common required arguments
    if not (opts.kafka_bootstrap_servers and opts.schema_registry_url and opts.target_db_server and
            opts.target_db_user and opts.target_db_password and opts.target_db_database):
        raise Exception('Arguments kafka_bootstrap_servers, schema_registry_url, target_db_server, '
                        'target_db_user, target_db_password, and target_db_database are all required.')

    # Validate all-changes-topic is provided (required for both modes)
    if not opts.all_changes_topic:
        raise Exception('Argument --all-changes-topic is required.')

    # Build list of replay configurations
    replay_configs: List[ReplayConfig] = []

    if opts.topic_to_table_map:
        # New mode: parallel replay of multiple topics
        topic_map = json.loads(opts.topic_to_table_map)
        for topic, table_info in topic_map.items():
            config = ReplayConfig(
                replay_topic=topic,
                target_db_table_schema=table_info.get('schema', 'dbo'),
                target_db_table_name=table_info['table'],
                cols_to_not_sync=table_info.get('cols_to_not_sync', ''),
                primary_key_fields_override=table_info.get('primary_key_fields_override', '')
            )
            replay_configs.append(config)
    elif opts.replay_topic and opts.target_db_table_schema and opts.target_db_table_name:
        # Legacy mode: single topic replay for backward compatibility
        config = ReplayConfig(
            replay_topic=opts.replay_topic,
            target_db_table_schema=opts.target_db_table_schema,
            target_db_table_name=opts.target_db_table_name,
            cols_to_not_sync=opts.cols_to_not_sync,
            primary_key_fields_override=opts.primary_key_fields_override
        )
        replay_configs.append(config)
    else:
        raise Exception('Either --topic-to-table-map OR (--replay-topic, --target-db-table-schema, '
                        'and --target-db-table-name) must be provided.')

    logger.info(f"Starting CDC replayer in {opts.mode} mode with {len(replay_configs)} topic(s).")

    if opts.mode == 'backfill':
        run_backfill_mode(opts, replay_configs)
    else:
        run_follow_mode(opts, replay_configs)


def run_backfill_mode(opts: argparse.Namespace, replay_configs: List[ReplayConfig]) -> None:
    """Run the replayer in backfill mode - parallel replay up to a cutoff LSN."""
    # Get cutoff LSN from the all-changes topic
    cutoff_lsn, cutoff_offset = get_latest_lsn_from_all_changes_topic(opts)
    logger.info(f"Backfill mode: will replay up to LSN {cutoff_lsn}")

    # Create shared structures for the consumer process
    stop_events: Dict[str, EventClass] = {}
    queues: Dict[str, Queue] = {}
    progress_by_topic: Dict[str, List[Progress]] = {}
    proc_id: str = f'{socket.getfqdn()}+{int(datetime.now().timestamp())}'

    # Get progress for each topic
    for config in replay_configs:
        stop_events[config.replay_topic] = mp.Event()
        # For faster_fifo the ctor arg here is the queue byte size, not its item count size:
        queues[config.replay_topic] = Queue(opts.upsert_batch_size * 5_000)

        # Need to fetch progress for each topic
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                         database=opts.target_db_database, timeout=30, login_timeout=30) as db_conn:
            worker_opts = argparse.Namespace(**vars(opts))
            worker_opts.replay_topic = config.replay_topic
            worker_opts.target_db_table_schema = config.target_db_table_schema
            worker_opts.target_db_table_name = config.target_db_table_name
            progress_by_topic[config.replay_topic] = get_progress(worker_opts, db_conn)

    # Launch single shared consumer process (with cutoff LSN for backfill mode)
    consumer_proc = mp.Process(
        target=backfill_consumer_process,
        name='shared-consumer',
        args=(replay_configs, opts, stop_events, queues, progress_by_topic, proc_id, cutoff_lsn, logger)
    )

    # Launch worker processes for each topic/table pair
    workers: List[mp.Process] = []
    backfill_completed = False
    try:
        consumer_proc.start()
        logger.info("Launched shared consumer process")

        for config in replay_configs:
            time.sleep(0.2)
            worker_proc_id = f'{proc_id}+{config.replay_topic}'
            worker = mp.Process(
                target=replay_worker,
                name=f'replayer-{config.replay_topic}',
                args=(config, opts, stop_events[config.replay_topic], queues[config.replay_topic],
                      worker_proc_id, cutoff_lsn)
            )
            worker.start()
            workers.append(worker)
            logger.debug(f"Launched worker process for '{config.replay_topic}' -> "
                        f"[{config.target_db_table_schema}].[{config.target_db_table_name}]")

        # Wait for all workers to complete
        for worker in workers:
            worker.join()

        # Wait for consumer to finish
        consumer_proc.join()

        backfill_completed = True
        logger.info("All replay workers have completed.")

        # Write progress for the all-changes topic so follow mode knows where to start
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                         database=opts.target_db_database, timeout=30, login_timeout=30) as db_conn:
            # Use current timestamp for the progress record
            commit_all_changes_topic_progress(opts, cutoff_offset, int(datetime.now().timestamp() * 1000),
                                              proc_id, db_conn)
            logger.info(f"Backfill complete. Wrote all-changes topic progress at offset {cutoff_offset} "
                       f"for follow mode handoff.")

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down workers...")
        for event in stop_events.values():
            event.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
        if consumer_proc.is_alive():
            consumer_proc.terminate()
        for worker in workers:
            worker.join(timeout=5)
        consumer_proc.join(timeout=5)
    except Exception as e:
        logger.exception(f"Error in main process: {e}")
        for event in stop_events.values():
            event.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
        if consumer_proc.is_alive():
            consumer_proc.terminate()
        for worker in workers:
            worker.join(timeout=5)
        consumer_proc.join(timeout=5)


def run_follow_mode(opts: argparse.Namespace, replay_configs: List[ReplayConfig]) -> None:
    """Run the replayer in follow mode - ordered replay from the all-changes topic."""
    proc_id: str = f'{socket.getfqdn()}+{int(datetime.now().timestamp())}'

    # Check for existing all-changes topic progress
    with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                     database=opts.target_db_database, timeout=30, login_timeout=30) as db_conn:
        all_changes_progress = get_all_changes_topic_progress(opts, db_conn)
        if all_changes_progress is None:
            raise Exception(f'No progress found for all-changes topic "{opts.all_changes_topic}" in namespace '
                           f'"{opts.progress_tracking_namespace}". Run backfill mode first to establish progress.')

        start_offset = all_changes_progress.last_handled_message_offset + 1
        logger.info(f"Follow mode: starting from all-changes topic offset {start_offset}")

    # Build topic-to-config mapping for routing messages
    topic_to_config: Dict[str, ReplayConfig] = {config.replay_topic: config for config in replay_configs}

    # Initialize Kafka consumer
    consumer_conf = {
        'bootstrap.servers': opts.kafka_bootstrap_servers,
        'group.id': f'replayer-follow-{proc_id}',
        'enable.auto.offset.store': False,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
        **opts.extra_kafka_consumer_config
    }
    consumer = Consumer(consumer_conf)

    # Initialize Avro deserializer
    schema_registry_client = SchemaRegistryClient({'url': opts.schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    # Assign to all-changes topic partition(s) at the saved offset
    topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
        topic=opts.all_changes_topic).topics  # type: ignore[call-arg]
    if topics_meta is None:
        raise Exception(f'No metadata found for topic {opts.all_changes_topic}')

    partitions = list((topics_meta[opts.all_changes_topic].partitions or {}).keys())
    if len(partitions) != 1:
        logger.warning(f'All-changes topic has {len(partitions)} partitions; expected 1 for ordered processing')

    topic_partitions = [TopicPartition(opts.all_changes_topic, p, start_offset) for p in partitions]
    consumer.assign(topic_partitions)

    logger.info(f"Follow mode: consuming from {opts.all_changes_topic}, assigned partitions: {partitions}")

    # Initialize per-table worker state
    # We'll use a simplified single-threaded approach that processes one message at a time
    # and maintains state per target table
    table_workers: Dict[str, 'FollowModeTableWorker'] = {}
    for config in replay_configs:
        table_workers[config.replay_topic] = FollowModeTableWorker(config, opts, proc_id)

    msg_ctr = 0
    last_all_changes_offset = start_offset - 1
    last_all_changes_timestamp = 0
    last_commit_time = datetime.now()

    try:
        while True:
            msg = consumer.poll(0.5)

            if msg is None:
                # Periodically commit progress even without new messages
                if (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds:
                    _flush_all_table_workers(table_workers, opts, proc_id)
                    if last_all_changes_offset >= start_offset:
                        with ctds.connect(opts.target_db_server, user=opts.target_db_user,
                                         password=opts.target_db_password, database=opts.target_db_database,
                                         timeout=30, login_timeout=30) as db_conn:
                            commit_all_changes_topic_progress(opts, last_all_changes_offset,
                                                             last_all_changes_timestamp, proc_id, db_conn)
                    last_commit_time = datetime.now()
                continue

            err = msg.error()
            if err:
                # noinspection PyProtectedMember
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise Exception(msg.error())

            msg_ctr += 1

            # Get the original topic from message header
            headers = msg.headers() or []
            original_topic = None
            for header_key, header_val in headers:
                if header_key == 'cdc_to_kafka_original_topic':
                    original_topic = header_val.decode('utf-8') if isinstance(header_val, bytes) else header_val
                    break

            if original_topic is None:
                logger.warning(f'Message at offset {msg.offset()} missing cdc_to_kafka_original_topic header, skipping')
                last_all_changes_offset = msg.offset()
                last_all_changes_timestamp = msg.timestamp()[1]
                continue

            if original_topic not in table_workers:
                # This table isn't in our replay config, skip it
                last_all_changes_offset = msg.offset()
                last_all_changes_timestamp = msg.timestamp()[1]
                continue

            # Deserialize the message
            raw_key = msg.key()
            raw_val = msg.value()

            msg_key: Optional[Dict[str, Any]] = None
            msg_val: Optional[Dict[str, Any]] = None
            if raw_key is not None:
                # Use the original topic for schema lookup
                msg_key = avro_deserializer(raw_key, SerializationContext(original_topic, MessageField.KEY))
            if raw_val is not None:
                msg_val = avro_deserializer(raw_val, SerializationContext(original_topic, MessageField.VALUE))

            # Process the message through the appropriate table worker
            worker = table_workers[original_topic]
            worker.process_message(msg_key, msg_val, msg.offset(), msg.timestamp()[1])

            last_all_changes_offset = msg.offset()
            last_all_changes_timestamp = msg.timestamp()[1]

            if msg_ctr % 5_000 == 0:
                logger.debug(f'Follow mode: processed {msg_ctr} messages, at offset {last_all_changes_offset}')

            # Periodically flush and commit
            if (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds:
                _flush_all_table_workers(table_workers, opts, proc_id)
                with ctds.connect(opts.target_db_server, user=opts.target_db_user,
                                 password=opts.target_db_password, database=opts.target_db_database,
                                 timeout=30, login_timeout=30) as db_conn:
                    commit_all_changes_topic_progress(opts, last_all_changes_offset,
                                                     last_all_changes_timestamp, proc_id, db_conn)
                last_commit_time = datetime.now()

            if 0 < opts.consumed_messages_limit <= msg_ctr:
                logger.info(f'Consumed {msg_ctr} messages, stopping...')
                break

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down follow mode...")
    finally:
        # Final flush
        _flush_all_table_workers(table_workers, opts, proc_id)
        if last_all_changes_offset >= start_offset:
            with ctds.connect(opts.target_db_server, user=opts.target_db_user,
                             password=opts.target_db_password, database=opts.target_db_database,
                             timeout=30, login_timeout=30) as db_conn:
                commit_all_changes_topic_progress(opts, last_all_changes_offset,
                                                 last_all_changes_timestamp, proc_id, db_conn)

        # Close table workers
        for worker in table_workers.values():
            worker.close()

        consumer.close()
        logger.info(f"Follow mode: processed {msg_ctr} messages total, final offset {last_all_changes_offset}")


def _flush_all_table_workers(table_workers: Dict[str, 'FollowModeTableWorker'],
                             opts: argparse.Namespace, proc_id: str) -> None:
    """Flush all table workers' queued operations."""
    for worker in table_workers.values():
        worker.flush(opts, proc_id)


class FollowModeTableWorker:
    """Manages state for a single table in follow mode."""

    def __init__(self, config: ReplayConfig, opts: argparse.Namespace, proc_id: str) -> None:
        self.config = config
        self.fq_target_table_name = f'[{config.target_db_table_schema.strip()}].[{config.target_db_table_name.strip()}]'
        self.cols_to_not_sync: set[str] = set([c.strip().lower() for c in config.cols_to_not_sync.split(',')])
        self.cols_to_not_sync.discard('')

        self.queued_deletes: Set[Any] = set()
        self.queued_upserts: Dict[Any, Tuple[str, List[Any]]] = {}
        self.last_offset_by_partition: Dict[int, Tuple[int, int]] = {}
        self.delete_cnt = 0
        self.upsert_cnt = 0

        # Initialize database connection and get table metadata
        self.db_conn = ctds.connect(opts.target_db_server, user=opts.target_db_user,
                                    password=opts.target_db_password, database=opts.target_db_database,
                                    timeout=15, login_timeout=15)

        with self.db_conn.cursor() as cursor:
            # Get primary key fields
            if config.primary_key_fields_override.strip():
                self.primary_key_field_names = [x.strip() for x in config.primary_key_fields_override.split(',')]
            else:
                cursor.execute('''
    SELECT [COLUMN_NAME]
    FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE]
    WHERE OBJECTPROPERTY(OBJECT_ID([CONSTRAINT_SCHEMA] + '.' + QUOTENAME([CONSTRAINT_NAME])), 'IsPrimaryKey') = 1
    AND [TABLE_SCHEMA] = :0
    AND [TABLE_NAME] = :1
    ORDER BY [ORDINAL_POSITION]
                ''', (config.target_db_table_schema, config.target_db_table_name))
                self.primary_key_field_names = [r[0] for r in cursor.fetchall()]

            # Get computed columns
            cursor.execute('''
SELECT name
FROM sys.computed_columns
WHERE OBJECT_SCHEMA_NAME(object_id) = :0
    AND OBJECT_NAME(object_id) = :1
            ''', (config.target_db_table_schema, config.target_db_table_name))
            computed_cols: set[str] = {r[0].lower() for r in cursor.fetchall()}

            # Get column metadata
            self.field_names: List[str] = []
            self.datetime_field_names: set[str] = set()
            self.varchar_field_names: set[str] = set()
            self.nvarchar_field_names: set[str] = set()
            self.column_defaults: Dict[str, Any] = {}
            delete_temp_table_col_specs: List[str] = []

            cursor.execute('''
SELECT [COLUMN_NAME]
    , [DATA_TYPE]
    , COALESCE([CHARACTER_MAXIMUM_LENGTH], [DATETIME_PRECISION]) AS [PRECISION_SPEC]
    , [IS_NULLABLE]
    , [COLUMN_DEFAULT]
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE [TABLE_SCHEMA] = :0
    AND [TABLE_NAME] = :1
ORDER BY [ORDINAL_POSITION]
            ''', (config.target_db_table_schema, config.target_db_table_name))

            for col_name, col_type, col_precision, col_is_nullable, col_default in cursor.fetchall():
                if col_name.lower() in self.cols_to_not_sync or col_name.lower() in computed_cols:
                    continue
                self.field_names.append(col_name)
                if col_type.lower().startswith('datetime'):
                    self.datetime_field_names.add(col_name)
                if col_type.lower() in ('char', 'varchar', 'text'):
                    self.varchar_field_names.add(col_name)
                if col_type.lower() in ('nchar', 'nvarchar', 'ntext'):
                    self.nvarchar_field_names.add(col_name)
                self.column_defaults[col_name] = parse_sql_default(col_default)
                if col_name in self.primary_key_field_names:
                    precision = f'({col_precision})' if col_precision is not None else ''
                    nullability = '' if col_is_nullable else 'NOT NULL'
                    delete_temp_table_col_specs.append(f'[{col_name}] {col_type}{precision} {nullability}')

            # Create temp tables
            temp_table_base_name = f'#replayer_{config.target_db_table_schema.strip()}_{config.target_db_table_name.strip()}'
            self.delete_temp_table_name = temp_table_base_name + '_delete'
            self.merge_temp_table_name = temp_table_base_name + '_merge'

            cursor.execute(f'DROP TABLE IF EXISTS {self.merge_temp_table_name};')
            cursor.execute(f'SELECT TOP 0 * INTO {self.merge_temp_table_name} FROM {self.fq_target_table_name} '
                          f'UNION ALL SELECT * FROM {self.fq_target_table_name} WHERE 1 <> 1;')
            for c in itertools.chain(self.cols_to_not_sync, computed_cols):
                cursor.execute(f'ALTER TABLE {self.merge_temp_table_name} DROP COLUMN [{c}];')

            cursor.execute(f'DROP TABLE IF EXISTS {self.delete_temp_table_name};')
            cursor.execute(f'''
CREATE TABLE {self.delete_temp_table_name} (
    {",".join(delete_temp_table_col_specs)},
    CONSTRAINT [PK_{self.delete_temp_table_name}]
    PRIMARY KEY ([{"], [".join(self.primary_key_field_names)}])
);
            ''')

            # Build SQL statements
            delete_join_predicates = ' AND '.join([f'tgt.[{c}] = dtt.[{c}]' for c in self.primary_key_field_names])
            self.delete_stmt = f'''
DELETE tgt
FROM {self.fq_target_table_name} AS tgt
INNER JOIN {self.delete_temp_table_name} AS dtt ON ({delete_join_predicates});

TRUNCATE TABLE {self.delete_temp_table_name};
            '''

            cursor.execute('SELECT TOP 1 [name] FROM sys.columns WHERE object_id = OBJECT_ID(:0) AND is_identity = 1',
                          (self.fq_target_table_name,))
            rows = cursor.fetchall()
            self.identity_col_name: Optional[str] = rows and rows[0][0] or None
            set_identity_insert_if_needed = f'SET IDENTITY_INSERT {self.fq_target_table_name} ON; ' \
                if self.identity_col_name else ''

            merge_match_predicates = ' AND '.join([f'tgt.[{c}] = src.[{c}]' for c in self.primary_key_field_names])

            if set(self.field_names) == set(self.primary_key_field_names):
                self.merge_stmt = f'''
                    {set_identity_insert_if_needed}
                    MERGE {self.fq_target_table_name} AS tgt
                    USING {self.merge_temp_table_name} AS src
                        ON ({merge_match_predicates})
                    WHEN NOT MATCHED THEN
                        INSERT ([{'], ['.join(self.field_names)}]) VALUES (src.[{'], src.['.join(self.field_names)}]);

                    TRUNCATE TABLE {self.merge_temp_table_name};
                '''
            else:
                self.merge_stmt = f'''
                    {set_identity_insert_if_needed}
                    MERGE {self.fq_target_table_name} AS tgt
                    USING {self.merge_temp_table_name} AS src
                        ON ({merge_match_predicates})
                    WHEN MATCHED THEN
                        UPDATE SET {", ".join([f'[{x}] = src.[{x}]' for x in self.field_names if x not in self.primary_key_field_names and x != self.identity_col_name])}
                    WHEN NOT MATCHED THEN
                        INSERT ([{'], ['.join(self.field_names)}]) VALUES (src.[{'], src.['.join(self.field_names)}]);

                    TRUNCATE TABLE {self.merge_temp_table_name};
                '''

        logger.info(f"Follow mode: initialized table worker for {self.fq_target_table_name}")

    def process_message(self, msg_key: Optional[Dict[str, Any]], msg_val: Optional[Dict[str, Any]],
                       offset: int, timestamp: int) -> None:
        """Process a single message for this table."""
        if msg_key is None:
            return

        key_val = tuple((msg_key[x] for x in self.primary_key_field_names))

        if msg_val is None or msg_val.get('__operation') == 'Delete':
            self.queued_deletes.add(key_val)
            self.queued_upserts.pop(key_val, None)
            self.delete_cnt += 1
        else:
            vals: List[Any] = []
            for f in self.field_names:
                if f not in msg_val:
                    vals.append(self.column_defaults[f])
                elif msg_val[f] is None:
                    vals.append(None)
                elif f in self.datetime_field_names:
                    dt = datetime.fromisoformat(msg_val[f])
                    if dt.year < 1753:
                        dt = datetime(1753, 1, 1, 0, 0, 0)
                    vals.append(dt)
                elif f in self.varchar_field_names:
                    vals.append(ctds.SqlVarChar(msg_val[f].encode('cp1252')))
                elif f in self.nvarchar_field_names:
                    vals.append(ctds.SqlVarChar(msg_val[f].encode('utf-16le')))
                else:
                    vals.append(msg_val[f])
            self.queued_upserts[key_val] = (msg_val['__operation'], vals)
            self.upsert_cnt += 1

        # Track progress per partition (use 0 since all-changes topic is single partition)
        self.last_offset_by_partition[0] = (offset, timestamp)

    def flush(self, opts: argparse.Namespace, proc_id: str) -> None:
        """Flush queued operations to the database."""
        if not self.queued_deletes and not self.queued_upserts:
            return

        with self.db_conn.cursor() as cursor:
            if self.queued_deletes:
                self.db_conn.bulk_insert(self.delete_temp_table_name, list(self.queued_deletes))
                cursor.execute(self.delete_stmt)
                logger.debug(f'Follow mode worker {self.config.replay_topic}: deleted {len(self.queued_deletes)} rows')

            if self.queued_upserts:
                merges = [val for _, (op, val) in self.queued_upserts.items()]
                self.db_conn.bulk_insert(self.merge_temp_table_name, merges)
                cursor.execute(self.merge_stmt)
                logger.debug(f'Follow mode worker {self.config.replay_topic}: merged {len(merges)} rows')

            self.queued_deletes.clear()
            self.queued_upserts.clear()

            # Commit per-table progress
            worker_opts = argparse.Namespace(**vars(opts))
            worker_opts.replay_topic = self.config.replay_topic
            worker_opts.target_db_table_schema = self.config.target_db_table_schema
            worker_opts.target_db_table_name = self.config.target_db_table_name
            commit_progress(worker_opts, self.last_offset_by_partition, proc_id, self.db_conn)

    def close(self) -> None:
        """Close the database connection."""
        self.db_conn.close()
        logger.info(f"Follow mode worker {self.config.replay_topic}: "
                   f"processed {self.delete_cnt} deletes, {self.upsert_cnt} upserts")


def backfill_consumer_process(replay_configs: List[ReplayConfig], opts: argparse.Namespace,
                              stop_events: Dict[str, EventClass], queues: Dict[str, Queue],
                              progress_by_topic: Dict[str, List[Progress]], proc_id: str,
                              cutoff_lsn: LsnPosition, logger: logging.Logger) -> None:
    """Consumer process for backfill mode - reads multiple topics and routes to workers.

    The cutoff_lsn parameter is accepted but not used here - workers check the cutoff themselves
    after deserializing messages (to avoid double-deserialization performance penalty).
    """
    consumer_conf = {'bootstrap.servers': opts.kafka_bootstrap_servers,
                     'group.id': f'unused-{proc_id}',
                     'enable.auto.offset.store': False,
                     'enable.auto.commit': False,  # We don't use Kafka for offset management in this code
                     'enable.partition.eof': True,
                     'auto.offset.reset': "earliest",
                     'on_commit': commit_cb,
                     **opts.extra_kafka_consumer_config}

    consumer: Consumer = Consumer(consumer_conf)

    # Build topic partitions for all topics and track high watermarks for EOF detection
    all_topic_partitions: List[TopicPartition] = []
    high_watermarks: Dict[str, Dict[int, int]] = {}  # topic -> partition -> high watermark
    for config in replay_configs:
        topic = config.replay_topic
        progress = progress_by_topic.get(topic, [])
        start_offset_by_partition: Dict[int, int] = {
            p.source_topic_partition: p.last_handled_message_offset + 1 for p in progress
        }
        topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
            topic=topic).topics  # type: ignore[call-arg]
        if topics_meta is None:
            raise Exception(f'No partitions found for topic {topic}')
        else:
            partitions = list((topics_meta[topic].partitions or {}).keys())

        # Get high watermarks for each partition to detect EOF
        high_watermarks[topic] = {}
        for p in partitions:
            tp = TopicPartition(topic, p)
            _, high = consumer.get_watermark_offsets(tp)
            high_watermarks[topic][p] = high
            logger.debug(f'Topic {topic} partition {p}: high watermark = {high}')

        topic_partitions: List[TopicPartition] = [TopicPartition(
            topic, p, start_offset_by_partition.get(p, OFFSET_BEGINNING)
        ) for p in partitions]
        all_topic_partitions.extend(topic_partitions)

    logger.debug('Consumer assignments: %s', [f'{tp.topic}:{tp.partition}@{tp.offset}' for tp in all_topic_partitions])
    consumer.assign(all_topic_partitions)
    msg_ctr: int = 0
    msg_ctr_by_topic: Dict[str, int] = {config.replay_topic: 0 for config in replay_configs}

    # Track which topics have been paused (worker hit cutoff)
    paused_topics: Set[str] = set()

    # Track which topic/partitions have reached EOF (for topics with infrequent messages)
    eof_partitions: Dict[str, Set[int]] = {config.replay_topic: set() for config in replay_configs}
    topics_at_eof: Set[str] = set()

    # Map topic names to their partitions for pausing
    topic_to_partitions: Dict[str, List[TopicPartition]] = {}
    for tp in all_topic_partitions:
        if tp.topic not in topic_to_partitions:
            topic_to_partitions[tp.topic] = []
        topic_to_partitions[tp.topic].append(TopicPartition(tp.topic, tp.partition))

    logger.debug(f'Starting shared consumer for topics: {[c.replay_topic for c in replay_configs]}')

    # Check if all workers have stopped
    def all_workers_stopped() -> bool:
        return all(event.is_set() for event in stop_events.values())

    while not all_workers_stopped():
        msg = consumer.poll(0.1)

        if msg is None:
            # Check if any workers have stopped and we need to pause their topics
            for topic, event in stop_events.items():
                if event.is_set() and topic not in paused_topics:
                    logger.info(f'Consumer: worker for topic {topic} has stopped, pausing partition(s)')
                    if topic in topic_to_partitions:
                        consumer.pause(topic_to_partitions[topic])
                    paused_topics.add(topic)
            continue

        msg_ctr += 1

        err = msg.error()
        if err:
            # noinspection PyProtectedMember
            if err.code() == KafkaError._PARTITION_EOF:
                # Track which partitions have reached EOF
                eof_topic = msg.topic()
                eof_partition = msg.partition()
                if eof_topic and eof_topic not in topics_at_eof:
                    eof_partitions[eof_topic].add(eof_partition)
                    # Check if all partitions for this topic have reached EOF
                    if eof_partitions[eof_topic] == set(high_watermarks[eof_topic].keys()):
                        logger.info(f'Consumer: all partitions for topic {eof_topic} have reached EOF, '
                                   f'sending EOF sentinel to worker')
                        topics_at_eof.add(eof_topic)
                        # Send EOF sentinel to the worker (None values signal EOF)
                        if eof_topic in queues:
                            queues[eof_topic].put((eof_topic, None, None, None, None, None))
                continue
            else:
                raise Exception(msg.error())

        topic = msg.topic()
        if topic is None:
            raise Exception('Unexpected None value for message topic()')

        # Check if this topic's worker has stopped (hit cutoff) - skip and pause if so
        if topic in stop_events and stop_events[topic].is_set():
            if topic not in paused_topics:
                logger.info(f'Consumer: worker for topic {topic} has stopped, pausing partition(s)')
                if topic in topic_to_partitions:
                    consumer.pause(topic_to_partitions[topic])
                paused_topics.add(topic)
            continue

        # Route message to the appropriate worker queue
        if topic not in queues:
            logger.warning(f'Received message for unexpected topic: {topic}')
            continue

        msg_ctr_by_topic[topic] += 1

        if msg_ctr_by_topic[topic] % 5_000 == 0:
            logger.debug(f'Topic {topic}: Reached %s, apx queue depth %s',
                        format_coordinates(msg), queues[topic].qsize())

        # Pass raw bytes to workers for deserialization
        raw_key = msg.key()
        raw_val = msg.value()

        retries: int = 0
        while True:
            try:
                queues[topic].put((topic, msg.partition(), msg.offset(), msg.timestamp()[1], raw_key, raw_val))
                break
            except stdlib_queue.Full:
                if retries < 5:
                    retries += 1
                    time.sleep(3)
                else:
                    raise

        if 0 < opts.consumed_messages_limit <= msg_ctr:
            logger.info(f'Consumed %s messages total, stopping...', opts.consumed_messages_limit)
            break

    # Set all stop events
    for event in stop_events.values():
        event.set()

    # Close all queues
    for queue in queues.values():
        queue.close()

    logger.info("Closing shared consumer.")
    consumer.close()

    for queue in queues.values():
        queue.join_thread()


if __name__ == "__main__":
    main()
