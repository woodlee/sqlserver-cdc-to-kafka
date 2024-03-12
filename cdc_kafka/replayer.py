#!/usr/bin/env python3

"""
A tool for populating a table that has been pre-created in a SQL Server DB based on data from a topic that was
produced by CDC-to-Kafka.

One example use case would be to create a copy of an existing table (initially with a different name of course) on the
same DB from which a CDC-to-Kafka topic is produced, in order to rearrange indexes, types, etc. on the new copy of the
table. We're using this now to upgrade some INT columns to `BIGINT`s on a few tables that are nearing the 2^31 row
count. When the copy is ready, tables can be renamed so that applications begin using the new table.

An example invocation follows. This assumes you have already created the Orders_copy table, with changes as desired,
in the DB, and that you have created the CdcTableCopier DB user there as well, presumably with limited permissions
to work only with the Orders_copy table. In this example we assume that 'OrderGuid' is a new column that exists
on the new _copy table only (perhaps populated by a default), and therefore we are not trying to sync that column
since it doesn't exist in the CDC feed/schema:

./replayer.py \
  --replay-topic 'dbo_Orders_cdc' \
  --kafka-bootstrap-servers 'localhost:9092' \
  --schema-registry-url 'http://localhost:8081' \
  --target-db-server 'localhost' \
  --target-db-user 'CdcTableCopier' \
  --target-db-password '*****' \
  --target-db-database 'MyDatabase' \
  --target-db-table-schema 'dbo' \
  --target-db-table-name 'Orders_copy'
  --cols-to-not-sync 'OrderGuid'

Python package requirements:
    confluent-kafka[avro]==2.3.0
    ctds==1.14.0
    faster-fifo==1.4.5

"""

import argparse
import logging.config
import multiprocessing as mp
import os
import queue as stdlib_queue
import socket
import time
from datetime import datetime, UTC
from multiprocessing.synchronize import Event as EventClass
from typing import Set, Any, List, Dict, Tuple, NamedTuple

import ctds
from confluent_kafka import Consumer, KafkaError, TopicPartition, Message, OFFSET_BEGINNING
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from faster_fifo import Queue

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

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


def main() -> None:
    p = argparse.ArgumentParser()

    # Config for data source
    p.add_argument('--replay-topic',
                   default=os.environ.get('REPLAY_TOPIC'))
    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))

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
                   default=os.environ.get('TARGET_DB_TABLE_SCHEMA'))
    p.add_argument('--target-db-table-name',
                   default=os.environ.get('TARGET_DB_TABLE_NAME'))
    p.add_argument('--cols-to-not-sync',
                   default=os.environ.get('COLS_TO_NOT_SYNC', ''))
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

    opts = p.parse_args()

    if not (opts.replay_topic and opts.kafka_bootstrap_servers and opts.schema_registry_url and
            opts.target_db_server and opts.target_db_user and opts.target_db_password and opts.target_db_database and
            opts.target_db_table_schema and opts.target_db_table_name):
        raise Exception('Arguments replay_topic, kafka_bootstrap_servers, schema_registry_url, target_db_server, '
                        'target_db_user, target_db_password, target_db_database, target_db_table_schema, and '
                        'target_db_table_name are all required.')

    msg_ctr: int = 0
    delete_cnt: int = 0
    upsert_cnt: int = 0
    poll_time_acc: float = 0.0
    sql_time_acc: float = 0.0
    queue_get_wait: float = 0.0
    last_commit_time: datetime = datetime.now()
    last_consume_by_partition: Dict[int, Tuple[int, int]] = {}
    queued_deletes: Set[Any] = set()
    queued_upserts: Dict[Any, Tuple[str, List[Any]]] = {}
    fq_target_table_name: str = f'[{opts.target_db_table_schema.strip()}].[{opts.target_db_table_name.strip()}]'
    temp_table_base_name: str = f'#replayer_{opts.target_db_table_schema.strip()}_{opts.target_db_table_name.strip()}'
    delete_temp_table_name: str = temp_table_base_name + '_delete'
    merge_temp_table_name: str = temp_table_base_name + '_merge'
    cols_to_not_sync: set[str] = set([c.strip().lower() for c in opts.cols_to_not_sync.split(',')])
    cols_to_not_sync.remove('')
    proc_id: str = f'{socket.getfqdn()}+{int(datetime.now().timestamp())}'
    stop_event: EventClass = mp.Event()
    # For faster_fifo the ctor arg here is the queue byte size, not its item count size:
    queue: Queue = Queue(opts.upsert_batch_size * 5_000)
    proc_start_time: float = time.perf_counter()

    logger.info("Starting CDC replayer.")

    try:
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                          database=opts.target_db_database) as db_conn:
            with db_conn.cursor() as cursor:
                progress: List[Progress] = get_progress(opts, db_conn)
                consumer_subprocess: mp.Process = mp.Process(
                    target=consumer_process, name='consumer', args=(opts, stop_event, queue, progress, proc_id, logger))
                consumer_subprocess.start()

                cursor.execute(f'''
SELECT [COLUMN_NAME]
FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE]
WHERE OBJECTPROPERTY(OBJECT_ID([CONSTRAINT_SCHEMA] + '.' + QUOTENAME([CONSTRAINT_NAME])), 'IsPrimaryKey') = 1
AND [TABLE_SCHEMA] = :0 
AND [TABLE_NAME] = :1
ORDER BY [ORDINAL_POSITION]
                ''', (opts.target_db_table_schema, opts.target_db_table_name))
                primary_key_field_names: List[str] = [r[0] for r in cursor.fetchall()]

                field_names: List[str] = []
                datetime_field_names: set[str] = set()
                varchar_field_names: set[str] = set()
                nvarchar_field_names: set[str] = set()
                delete_temp_table_col_specs: List[str] = []
                cursor.execute('''
SELECT [COLUMN_NAME]
    , [DATA_TYPE]
    , COALESCE([CHARACTER_MAXIMUM_LENGTH], [DATETIME_PRECISION]) AS [PRECISION_SPEC]
    , [IS_NULLABLE]
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE [TABLE_SCHEMA] = :0 
    AND [TABLE_NAME] = :1
ORDER BY [ORDINAL_POSITION]
                ''', (opts.target_db_table_schema, opts.target_db_table_name))
                for col_name, col_type, col_precision, col_is_nullable in cursor.fetchall():
                    if col_name.lower() in cols_to_not_sync:
                        continue
                    field_names.append(col_name)
                    if col_type.lower().startswith('datetime'):  # TODO: more cases than "datetime"?
                        datetime_field_names.add(col_name)
                    if col_type.lower() in ('char', 'varchar', 'text'):
                        varchar_field_names.add(col_name)
                    if col_type.lower() in ('nchar', 'nvarchar', 'ntext'):
                        nvarchar_field_names.add(col_name)
                    if col_name in primary_key_field_names:
                        precision: str = f'({col_precision})' if col_precision is not None else ''
                        nullability: str = '' if col_is_nullable else 'NOT NULL'
                        delete_temp_table_col_specs.append(f'{col_name} {col_type}{precision} {nullability}')

                cursor.execute(f'DROP TABLE IF EXISTS {merge_temp_table_name};')
                # Yep, this looks weird--it's a hack to prevent SQL Server from copying over the IDENTITY property
                # of any columns that have it whenever it creates the temp table. https://stackoverflow.com/a/57509258
                cursor.execute(f'SELECT TOP 0 * INTO {merge_temp_table_name} FROM {fq_target_table_name} '
                               f'UNION ALL SELECT * FROM {fq_target_table_name} WHERE 1 <> 1;')
                for c in cols_to_not_sync:
                    cursor.execute(f'ALTER TABLE {merge_temp_table_name} DROP COLUMN {c};')

                cursor.execute(f'DROP TABLE IF EXISTS {delete_temp_table_name};')
                cursor.execute(f'''
CREATE TABLE {delete_temp_table_name} (
    {",".join(delete_temp_table_col_specs)}, 
    CONSTRAINT [PK_{delete_temp_table_name}] 
    PRIMARY KEY ({",".join(primary_key_field_names)})
);
                ''')

                delete_join_predicates: str = ' AND '.join([f'tgt.[{c}] = dtt.[{c}]' for c in primary_key_field_names])
                delete_stmt: str = f'''
DELETE tgt
FROM {fq_target_table_name} AS tgt
INNER JOIN {delete_temp_table_name} AS dtt ON ({delete_join_predicates});

TRUNCATE TABLE {delete_temp_table_name};
                '''

                cursor.execute('SELECT TOP 1 1 FROM sys.columns WHERE object_id = OBJECT_ID(:0) AND is_identity = 1',
                               (fq_target_table_name,))
                set_identity_insert_if_needed = f'SET IDENTITY_INSERT {fq_target_table_name} ON; ' \
                    if len(cursor.fetchall()) else ''

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
    UPDATE SET {", ".join([f'[{x}] = src.[{x}]' for x in field_names if x not in primary_key_field_names])}
WHEN NOT MATCHED THEN
    INSERT ([{'], ['.join(field_names)}]) VALUES (src.[{'], src.['.join(field_names)}]);

TRUNCATE TABLE {merge_temp_table_name};
                    '''

            proc_start_time = time.perf_counter()
            logger.info("Listening for consumed messages.")

            while True:
                queue_get_wait_start = time.perf_counter()
                try:
                    msg_partition, msg_offset, msg_timestamp, msg_key, msg_val = queue.get(timeout=0.01)
                except stdlib_queue.Empty:
                    msg_partition, msg_offset, msg_timestamp, msg_key, msg_val = None, None, None, None, None
                queue_get_wait += time.perf_counter() - queue_get_wait_start

                if len(queued_deletes) >= opts.delete_batch_size or len(queued_upserts) >= opts.upsert_batch_size or \
                        (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds:
                    with db_conn.cursor() as cursor:
                        if queued_deletes:
                            start_time = time.perf_counter()
                            db_conn.bulk_insert(delete_temp_table_name, list(queued_deletes))
                            sql_time_acc += time.perf_counter() - start_time
                            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            logger.info('Inserted to temp table for delete: %s items in %s ms',
                                        len(queued_deletes), elapsed_ms)

                            start_time = time.perf_counter()
                            cursor.execute(delete_stmt)
                            sql_time_acc += time.perf_counter() - start_time
                            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            logger.info('Deleted %s items in %s ms', len(queued_deletes), elapsed_ms)

                        if queued_upserts:
                            inserts: List[Any] = []
                            merges: List[Any] = []
                            for _, (op, val) in queued_upserts.items():
                                if op in ('Snapshot', 'Insert'):
                                    inserts.append(val)
                                else:
                                    merges.append(val)

                            # Inserts CAN be treated as merges, and it's probably more efficient to do so if there
                            # are relatively few of them, to cut down on DB round-trips:
                            if inserts and merges and len(inserts) < 100 or \
                                    len(inserts) / (len(inserts) + len(merges)) < 0.1:
                                logger.debug('Rolling %s inserts into %s merges', len(inserts), len(merges))
                                merges += inserts
                                inserts = []

                            if inserts:
                                start_time = time.perf_counter()
                                db_conn.bulk_insert(fq_target_table_name, inserts)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Inserted directly to target table: %s items in %s ms',
                                            len(inserts), elapsed_ms)

                            if merges:
                                start_time = time.perf_counter()
                                db_conn.bulk_insert(merge_temp_table_name, merges)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Inserted to temp table for merge: %s items in %s ms',
                                            len(merges), elapsed_ms)

                                start_time = time.perf_counter()
                                cursor.execute(merge_stmt)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Merged %s items in %s ms', len(merges), elapsed_ms)

                        if queued_upserts or queued_deletes:
                            queued_upserts.clear()
                            queued_deletes.clear()
                            commit_progress(opts, last_consume_by_partition, proc_id, db_conn)
                            last_commit_time = datetime.now()

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
                        if f in datetime_field_names and msg_val[f] is not None:
                            vals.append(datetime.fromisoformat(msg_val[f]))
                        elif f in varchar_field_names and msg_val[f] is not None:
                            # The below assumes your DB uses SQL_Latin1_General_CP1_CI_AS collation; if not, you may
                            # need to change 'cp1252' to something else:
                            vals.append(ctds.SqlVarChar(msg_val[f].encode('cp1252')))
                        elif f in nvarchar_field_names and msg_val[f] is not None:
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

                if len(queued_deletes) + len(queued_upserts) % 5_000 == 0:
                    logger.debug('Currently have %s queued deletes and %s queued upserts. Time waiting on queue = %s. '
                                 'Apx. queue depth %s', len(queued_deletes), len(queued_upserts), queue_get_wait,
                                 queue.qsize())
                    queue_get_wait = 0

                last_consume_by_partition[msg_partition] = (msg_offset, msg_timestamp)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(e)
    finally:
        stop_event.set()
        consumer_subprocess.join(1)
        logger.info(f'Processed {msg_ctr} messages total, {delete_cnt} deletes, {upsert_cnt} upserts.')
        overall_time = time.perf_counter() - proc_start_time
        logger.info(f'Total times:\nKafka poll: {poll_time_acc:.2f}s\nSQL execution: {sql_time_acc:.2f}s\n'
                    f'Overall: {overall_time:.2f}s')


def consumer_process(opts: argparse.Namespace, stop_event: EventClass, queue: Queue,
                     progress: List[Progress], proc_id: str, logger: logging.Logger) -> None:
    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient({'url': opts.schema_registry_url})
    avro_deserializer: AvroDeserializer = AvroDeserializer(schema_registry_client)
    consumer_conf = {'bootstrap.servers': opts.kafka_bootstrap_servers,
                     'group.id': f'unused-{proc_id}',
                     'enable.auto.offset.store': False,
                     'enable.auto.commit': False,  # We don't use Kafka for offset management in this code
                     'auto.offset.reset': "earliest",
                     'on_commit': commit_cb}
    consumer: Consumer = Consumer(consumer_conf)
    start_offset_by_partition: Dict[int, int] = {
        p.source_topic_partition: p.last_handled_message_offset + 1 for p in progress
    }
    partitions: List[int] = consumer.list_topics(topic=opts.replay_topic).topics[opts.replay_topic].partitions
    toppars: List[TopicPartition] = [TopicPartition(
        opts.replay_topic, p, start_offset_by_partition.get(p, OFFSET_BEGINNING)
    ) for p in partitions]
    logger.info('Consumer assignments: %s', [f'{tp.topic}:{tp.partition}@{tp.offset}' for tp in toppars])
    consumer.assign(toppars)
    msg_ctr: int = 0

    logger.info(f'Starting consumer for {opts.replay_topic}.')

    while not stop_event.is_set():
        msg = consumer.poll(0.01)

        if msg is None:
            continue

        msg_ctr += 1

        if msg_ctr % 5_000 == 0:
            logger.debug(f'Reached %s, apx queue depth %s', format_coordinates(msg), queue.qsize())

        if msg.error():
            # noinspection PyProtectedMember
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            else:
                raise Exception(msg.error())

        # noinspection PyArgumentList,PyTypeChecker
        msg_key: Dict[str, Any] = avro_deserializer(
            msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
        # noinspection PyArgumentList,PyTypeChecker
        msg_val: Dict[str, Any] = avro_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

        queue.put((msg.partition(), msg.offset(), msg.timestamp()[1], msg_key, msg_val))

        if 0 < opts.consumed_messages_limit <= msg_ctr:
            logger.info(f'Consumed %s messages, stopping...', opts.consumed_messages_limit)
            break

    queue.close()
    stop_event.set()
    logger.info("Closing consumer.")
    consumer.close()
    queue.join_thread()


if __name__ == "__main__":
    main()
