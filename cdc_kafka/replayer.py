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
  --schema-registry-url 'http://localhost:8081' \
  --kafka-bootstrap-servers 'localhost:9092' \
  --target-db-conn-string 'DRIVER=ODBC Driver 18 for SQL Server; TrustServerCertificate=yes; SERVER=localhost; DATABASE=mydb; UID=CdcTableCopier; PWD=*****; APP=cdc-to-kafka-replayer;' \
  --target-db-schema 'dbo' \
  --target-db-table 'Orders_copy' \
  --cols-to-not-sync 'OrderGuid' \
  --consumer-group-name 'cdc-to-kafka-replayer-1'

"""

import argparse
import logging.config
import os
import time
from datetime import datetime
from typing import Set, Any, List, Dict

import pyodbc

from confluent_kafka import Consumer, KafkaError, TopicPartition, Message
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

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
            'format': '%(asctime)s %(levelname)-8s [%(name)s:%(lineno)s] %(message)s',
        },
    },
})

logger = logging.getLogger(__name__)


DELETE_BATCH_SIZE = 2000
MERGE_BATCH_SIZE = 5000
MAX_BATCH_LATENCY_SECONDS = 10


def commit_cb(err: KafkaError, tps: List[TopicPartition]):
    if err is not None:
        logger.error(f'Error committing offsets: {err}')
    else:
        logger.debug(f'Offsets committed for {tps}')


def format_coordinates(msg: Message) -> str:
    return f'{msg.topic()}:{msg.partition()}@{msg.offset()}, ' \
           f'time {datetime.fromtimestamp(msg.timestamp()[1] / 1000)}'


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--replay-topic',
                   default=os.environ.get('REPLAY_TOPIC'))
    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))
    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--target-db-conn-string',
                   default=os.environ.get('TARGET_DB_CONN_STRING'))
    p.add_argument('--target-db-schema',
                   default=os.environ.get('TARGET_DB_SCHEMA'))
    p.add_argument('--target-db-table',
                   default=os.environ.get('TARGET_DB_TABLE'))
    p.add_argument('--cols-to-not-sync',
                   default=os.environ.get('COLS_TO_NOT_SYNC', ''))
    p.add_argument('--consumer-group-name',
                   default=os.environ.get('CONSUMER_GROUP_NAME', ''))

    opts = p.parse_args()

    logger.info("Starting CDC replayer.")
    proc_start_time = time.perf_counter()

    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.replay_topic and
            opts.target_db_conn_string and opts.target_db_schema and opts.target_db_table and
            opts.consumer_group_name):
        raise Exception('Arguments replay_topic, target_db_conn_string, target_db_schema, target_db_table, '
                        'schema_registry_url, consumer_group_name, and kafka_bootstrap_servers are all required.')

    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient({'url': opts.schema_registry_url})
    avro_deserializer: AvroDeserializer = AvroDeserializer(schema_registry_client)
    consumer_conf = {'bootstrap.servers': opts.kafka_bootstrap_servers,
                     'group.id': opts.consumer_group_name,
                     'enable.auto.offset.store': False,
                     'enable.auto.commit': False,
                     'auto.offset.reset': "earliest",
                     'on_commit': commit_cb}
    consumer: Consumer = Consumer(consumer_conf)
    consumer.subscribe([opts.replay_topic])
    logger.info(f'Subscribed to topic {opts.replay_topic}.')

    msg_ctr: int = 0
    del_cnt: int = 0
    merge_cnt: int = 0
    poll_time_acc: float = 0.0
    ser_time_acc: float = 0.0
    sql_time_acc: float = 0.0
    last_commit_time: datetime = datetime.now()
    queued_deletes: Set[Any] = set()
    queued_merges: Dict[Any, List[Any]] = {}
    fq_target_name: str = f'[{opts.target_db_schema.strip()}].[{opts.target_db_table.strip()}]'
    temp_table_name: str = f'#replayer_{opts.target_db_schema.strip()}_{opts.target_db_table.strip()}_bulk_load'
    cols_to_not_sync: set[str] = set(opts.cols_to_not_sync.split(','))

    try:
        with pyodbc.connect(opts.target_db_conn_string) as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute(f'''
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
    AND TABLE_NAME = ? AND TABLE_SCHEMA = ?
                ''', opts.target_db_table, opts.target_db_schema)

                pk_fields = [r[0] for r in cursor.fetchall()]
                if len(pk_fields) != 1:
                    raise Exception(f'Can only handle single-col PKs for now! Found: {pk_fields}')
                pk_field = pk_fields[0]

                odbc_columns = tuple(cursor.columns(
                    schema=opts.target_db_schema, table=opts.target_db_table).fetchall())

                param_types = [(x[4], x[6], x[8]) for x in odbc_columns]
                fields: List[str] = [c[3] for c in odbc_columns if c[3] not in cols_to_not_sync]
                quoted_fields_str: str = ', '.join((f'[{c}]' for c in fields))
                datetime_fields: set[str] = {c[3] for c in odbc_columns
                                             if c[4] in (pyodbc.SQL_TYPE_TIMESTAMP,)}  # TODO: more cases??

                cursor.execute(f'DROP TABLE IF EXISTS {temp_table_name};')
                # Yep, this looks weird--it's a hack to prevent SQL Server from copying over the IDENTITY property
                # of any columns that have it whenever it creates the temp table. https://stackoverflow.com/a/57509258
                cursor.execute(f'SELECT TOP 0 * INTO {temp_table_name} FROM {fq_target_name} '
                               f'UNION ALL SELECT * FROM {fq_target_name} WHERE 1 <> 1;')
                for c in opts.cols_to_not_sync.split(','):
                    if not c.strip():
                        continue
                    cursor.execute(f'ALTER TABLE {temp_table_name} DROP COLUMN {c.strip()};')
                cursor.execute('SELECT TOP 1 1 FROM sys.columns WHERE object_id = OBJECT_ID(?) AND is_identity = 1',
                               fq_target_name)
                set_identity_insert_if_needed = f'SET IDENTITY_INSERT {fq_target_name} ON; ' \
                    if bool(cursor.fetchval()) else ''

            logger.info("Starting to consume messages.")
            while True:
                start_time = time.perf_counter()
                msg = consumer.poll(0.5)
                poll_time_acc += time.perf_counter() - start_time

                if len(queued_deletes) >= DELETE_BATCH_SIZE or len(queued_merges) >= MERGE_BATCH_SIZE or \
                        (datetime.now() - last_commit_time).seconds > MAX_BATCH_LATENCY_SECONDS:
                    if queued_deletes:
                        q = None
                        try:
                            with db_conn.cursor() as cursor:
                                start_time = time.perf_counter()
                                q = f'''
                                    DELETE FROM {fq_target_name}
                                    WHERE [{pk_field}] IN ({'?,' * (len(queued_deletes) - 1)}?);
                                '''
                                cursor.execute(q, list(queued_deletes))
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                        except Exception as e:
                            print(q)
                            raise e

                        logger.info('Deleted %s items in %s ms', len(queued_deletes), elapsed_ms)
                    if queued_merges:
                        q = None
                        try:
                            with db_conn.cursor() as cursor:
                                data = list(queued_merges.values())
                                start_time = time.perf_counter()
                                cursor.fast_executemany = True
                                cursor.setinputsizes(param_types)
                                q = f'''
                                    INSERT INTO {temp_table_name} ({quoted_fields_str})
                                    VALUES({'?,' * (len(fields) - 1)}?);
                                '''
                                cursor.executemany(q, data)
                                q = f'''
                                    {set_identity_insert_if_needed}
                                    MERGE {fq_target_name} AS tgt
                                    USING {temp_table_name} AS src
                                    ON (tgt.[{pk_field}] = src.[{pk_field}])
                                    WHEN MATCHED THEN
                                        UPDATE SET {", ".join([f'[{x}] = src.[{x}]' for x in fields if x != pk_field])}
                                    WHEN NOT MATCHED THEN 
                                        INSERT ({quoted_fields_str}) VALUES (src.[{'], src.['.join(fields)}]);
                                    TRUNCATE TABLE {temp_table_name};
                                '''
                                cursor.execute(q)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                        except Exception as e:
                            print(q)
                            raise e

                        logger.info('Merged %s items in %s ms', len(queued_merges), elapsed_ms)
                    if queued_merges or queued_deletes:
                        queued_merges.clear()
                        queued_deletes.clear()
                        consumer.commit()
                        last_commit_time = datetime.now()

                if msg is None:
                    continue

                msg_ctr += 1

                if msg_ctr % 100_000 == 0:
                    logger.info(f'Reached %s', format_coordinates(msg))

                if msg.error():
                    # noinspection PyProtectedMember
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    else:
                        raise Exception(msg.error())

                start_time = time.perf_counter()
                # noinspection PyTypeChecker
                key: Dict[str, Any] = avro_deserializer(
                    msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                # noinspection PyArgumentList,PyTypeChecker
                val: Dict[str, Any] = avro_deserializer(
                    msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                ser_time_acc += time.perf_counter() - start_time

                pk_val = key[pk_field]

                if val is None or val['__operation'] == 'Delete':
                    queued_deletes.add(pk_val)
                    queued_merges.pop(pk_val, None)
                    del_cnt += 1
                else:
                    vals = []
                    for f in fields:
                        if f in datetime_fields and val[f] is not None:
                            vals.append(datetime.fromisoformat(val[f]))
                        else:
                            vals.append(val[f])
                    queued_merges[pk_val] = vals
                    queued_deletes.discard(pk_val)
                    merge_cnt += 1

                consumer.store_offsets(msg)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(e)

    logger.info(f'Processed {msg_ctr} messages total, {del_cnt} deletes, {merge_cnt} merges.')
    overall_time = time.perf_counter() - proc_start_time
    logger.info(f'Total times:\nKafka poll: {poll_time_acc:.2f}s\nAvro deserialize: {ser_time_acc:.2f}s\n'
                f'SQL execution: {sql_time_acc:.2f}s\nOverall: {overall_time:.2f}s')
    logger.info("Closing consumer.")
    consumer.close()


if __name__ == "__main__":
    main()
