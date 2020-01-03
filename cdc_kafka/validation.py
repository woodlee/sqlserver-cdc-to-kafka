import logging

from tabulate import tabulate

from . import constants

logger = logging.getLogger(__name__)


class Validator(object):
    def __init__(self, kafka_client, db_conn, tables):
        self.kafka_client = kafka_client
        self.db_conn = db_conn
        self.tables = tables

    def run(self):
        data = []

        try:
            for table in self.tables:
                logger.info('Validation: consuming all records from topic %s', table.topic_name)

                kafka_total_tombstones = 0
                kafka_total_snapshots = 0
                kafka_total_inserts = 0
                kafka_total_updates = 0
                kafka_total_deletes = 0
                kafka_snapshot_msg_keys = set()
                kafka_change_msg_keys = set()

                for msg in self.kafka_client.consume_all(table.topic_name):
                    if msg.value() is None:
                        kafka_total_tombstones += 1
                        continue

                    op = msg.value()[constants.OPERATION_NAME]
                    key = tuple(msg.key().items())

                    if op == constants.DELETE_OPERATION_NAME:
                        kafka_change_msg_keys.discard(key)
                        kafka_total_deletes += 1
                    elif op == constants.SNAPSHOT_OPERATION_NAME:
                        kafka_snapshot_msg_keys.add(key)
                        kafka_total_snapshots += 1
                    elif op == constants.POST_UPDATE_OPERATION_NAME:
                        kafka_change_msg_keys.add(key)
                        kafka_total_updates += 1
                    elif op == constants.INSERT_OPERATION_NAME:
                        kafka_change_msg_keys.add(key)
                        kafka_total_inserts += 1
                    else:
                        raise Exception(f'Unrecognized CDC operation type {op}')

                db_delete_rows, db_insert_rows, db_update_rows = table.get_change_table_counts()
                db_source_rows = table.get_source_table_count()

                data.append((
                    table.topic_name,
                    kafka_total_tombstones,
                    kafka_total_deletes,
                    db_delete_rows,
                    kafka_total_inserts,
                    db_insert_rows,
                    kafka_total_updates,
                    db_update_rows,
                    kafka_total_snapshots,
                    db_source_rows,
                    len(kafka_snapshot_msg_keys),
                    len(kafka_change_msg_keys),
                    len(kafka_snapshot_msg_keys.union(kafka_change_msg_keys)),
                ))

            return 0
        finally:
            if data:
                headers = (
                    'Topic',
                    'Tombstone recs',
                    'Kafka delete recs',
                    'CDC delete rows',
                    'Kafka insert recs',
                    'CDC insert rows',
                    'Kafka update recs',
                    'CDC update rows',
                    'Kafka snapshot recs',
                    'DB source table rows',
                    'Kafka unique snapshot keys',
                    'Kafka unique change keys',
                    'Union total unique keys'
                )
                table = tabulate(data, headers, tablefmt='fancy_grid')
                print('Validation results:\n' + table)
