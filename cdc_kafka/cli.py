import argparse
import datetime
import json
import logging
import os
import queue
import time
from typing import List

import pyodbc
from tabulate import tabulate

from . import kafka, tracked_tables, constants

logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()

    # Required
    p.add_argument('--db-conn-string',
                   default=os.environ.get('DB_CONN_STRING'),
                   help='ODBC connection string for the DB from which you wish to consume CDC logs')

    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
                   help='Host and port for your Kafka cluster, e.g. "localhost:9092"')

    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'),
                   help='URL to your Confluent Schema Registry, e.g. "http://localhost:8081"')

    # Optional
    p.add_argument('--kafka-timeout-seconds',
                   type=int,
                   default=os.environ.get('KAFKA_TIMEOUT_SECONDS', 10),
                   help="Timeout to use when interacting with Kafka during startup for checking topic watermarks and "
                        "pulling messages to check existing progress.")

    p.add_argument('--extra-kafka-consumer-config',
                   nargs='*',
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG'),
                   help="Additional config parameters to be used when instantiating the Kafka consumer (used only for "
                        "checking current CDC-following position upon startup). Should be a colon-delimited key:value. "
                        "You may specify multiple times.")

    p.add_argument('--extra-kafka-producer-config',
                   nargs='*',
                   default=os.environ.get('EXTRA_KAFKA_PRODUCER_CONFIG'),
                   help="Additional config parameters to be used when instantiating the Kafka producer. Should be a "
                        "colon-delimited key:value. You may specify multiple times.")

    p.add_argument('--table-blacklist-regex',
                   default=os.environ.get('TABLE_BLACKLIST_REGEX'),
                   help="A regex used to blacklist any tables that are tracked by CDC in your DB, but for which you "
                        "don't wish to publish data using this tool. Tables names are specified in dot-separated "
                        "'schema_name.table_name' form. Applied after the whitelist, if specified.")

    p.add_argument('--table-whitelist-regex',
                   default=os.environ.get('TABLE_WHITELIST_REGEX'),
                   help="A regex used to whitelist the specific CDC-tracked tables in your DB that you wish to publish "
                        "data for with this tool. Tables names are specified in dot-separated 'schema_name.table_name' "
                        "form.")

    p.add_argument('--topic-name-template',
                   default=os.environ.get('TOPIC_NAME_TEMPLATE', '{schema_name}_{table_name}_cdc'),
                   help="Template by which the Kafka topics will be named. Uses curly braces to specify substituted "
                        "values. Values available for substitution are `schema_name`, `table_name`, and `capture_"
                        "instance_name`.")

    p.add_argument('--snapshot-table-blacklist-regex',
                   default=os.environ.get('SNAPSHOT_TABLE_BLACKLIST_REGEX'),
                   help="A regex used to blacklist any tables for which you don't want to do a full initial-snapshot "
                        "read, in the case that this tool is being applied against them for the first time. Table "
                        "names are specified in dot-separated 'schema_name.table_name' form. Applied after the "
                        "whitelist, if specified.")

    p.add_argument('--snapshot-table-whitelist-regex',
                   default=os.environ.get('SNAPSHOT_TABLE_WHITELIST_REGEX'),
                   help="A regex used to whitelist the specific tables for which you want to do a full initial-"
                        "snapshot read, in the case that this tool is being applied against them for the first time. "
                        "Tables names are specified in dot-separated 'schema_name.table_name' form.")

    p.add_argument('--capture-instance-version-strategy',
                   choices=('regex', 'create_date'),
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_STRATEGY', 'create_date'),
                   help="If there is more than one capture instance following a given source table, how do you want to "
                        "select which one this tool reads? `create_date` (the default) will follow the one most "
                        "recently created. `regex` allows you to specify a regex against the capture instance name "
                        "(as argument `capture-instance-version-regex`, the first captured group of which will be used "
                        "in a lexicographic ordering of capture instance names to select the highest one. This can be "
                        "useful if your capture instance names have a version number in them.")

    p.add_argument('--capture-instance-version-regex',
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_REGEX'),
                   help="Regex to use if specifying option `regex` for argument `capture-instance-version-strategy`")

    p.add_argument('--progress-topic-name',
                   default=os.environ.get('PROGRESS_TOPIC_NAME', '_cdc_to_kafka_progress'),
                   help="Name of the topic used to store progress details reading change tables (and also source "
                        "tables, in the case of snapshots). This process will create the topic if it does not yet "
                        "exist. It should have only one partition.")

    p.add_argument('--disable-deletion-tombstones',
                   action='store_true',
                   default=bool(os.environ.get('DISABLE_DELETION_TOMBSTONES', False)),
                   help="Name of the topic used to store progress details reading change tables (and also source "
                        "tables, in the case of snapshots). This process will create the topic if it does not yet "
                        "exist. It should have only one partition.")

    opts = p.parse_args()

    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.db_conn_string):
        raise Exception('Arguments schema_registry_url, kafka_bootstrap_servers, and db_conn_string are all required.')

    logger.debug('Parsed configuration: \n%s', json.dumps(vars(opts), indent=4))

    with kafka.KafkaClient(opts.kafka_bootstrap_servers,
                           opts.schema_registry_url,
                           opts.kafka_timeout_seconds,
                           opts.progress_topic_name,
                           tracked_tables.TrackedTable.progress_message_extractor,
                           opts.extra_kafka_consumer_config,
                           opts.extra_kafka_producer_config) as kafka_client, \
            pyodbc.connect(opts.db_conn_string) as db_conn:

        tables = tracked_tables.build_tracked_tables_from_cdc_metadata(db_conn,
                                                                       opts.topic_name_template,
                                                                       opts.table_whitelist_regex,
                                                                       opts.table_blacklist_regex,
                                                                       opts.snapshot_table_whitelist_regex,
                                                                       opts.snapshot_table_blacklist_regex,
                                                                       opts.capture_instance_version_strategy,
                                                                       opts.capture_instance_version_regex)

        determine_start_points_and_finalize_tables(kafka_client, tables)

        logger.info('Beginning processing for %s tracked tables.', len(tables))

        # Prioritization in this queue is based on the commit time of the change data rows pulled from SQL. It will
        # hold one entry per tracked table, which will be replaced with the next entry for that table after the
        # current one is pulled off:
        pq = queue.PriorityQueue(len(tables))

        for table in tables:
            priority, msg_key, msg_value = table.pop_next()
            pq.put((priority, msg_key, msg_value, table))

        last_published_counts_log_time = datetime.datetime.now()
        published_messages_ctr = 0

        while True:
            if (datetime.datetime.now() - last_published_counts_log_time) > constants.PUBLISHED_COUNTS_LOGGING_INTERVAL:
                logger.info('Published %s change rows in the last interval.', published_messages_ctr)
                last_published_counts_log_time = datetime.datetime.now()
                published_messages_ctr = 0

            _, msg_key, msg_value, table = pq.get()

            if msg_key is not None:
                kafka_client.produce(table.topic_name, msg_key, table.key_schema, msg_value, table.value_schema)
                if msg_value['_cdc_operation'] == 'Delete' and not opts.disable_deletion_tombstones:
                    kafka_client.produce(table.topic_name, msg_key, table.key_schema, None, table.value_schema)
                    published_messages_ctr += 1
                published_messages_ctr += 1

            # Put the next entry for the table just handled back on the priority queue:
            priority, msg_key, msg_value = table.pop_next()
            pq.put((priority, msg_key, msg_value, table))


def determine_start_points_and_finalize_tables(kafka_client: kafka.KafkaClient,
                                               tables: List[tracked_tables.TrackedTable]) -> None:
    topic_names = [t.topic_name for t in tables]

    first_check_watermarks = json.dumps(kafka_client.get_topic_watermarks(topic_names), indent=4)
    logger.info('Pausing briefly to ensure target topics are not receiving new messages from elsewhere...')
    time.sleep(constants.STABLE_WATERMARK_CHECKS_INTERVAL_SECONDS)

    # This takes a little time so we'll throw it in here too to up the delay between watermark checks:
    prior_progress = kafka_client.get_prior_progress_or_create_progress_topic()

    second_check_watermarks = json.dumps(kafka_client.get_topic_watermarks(topic_names), indent=4)

    if first_check_watermarks != second_check_watermarks:
        raise Exception(f'Watermarks for one or more target topics changed between successive checks. '
                        f'Another process may be producing to the topic(s). Bailing.\nFirst check: '
                        f'{first_check_watermarks}\nSecond check: {second_check_watermarks}')

    prior_progress_log_table_data = []

    for table in tables:
        prior_change_rows_progress = prior_progress.get((
            ('capture_instance_name', table.capture_instance_name),
            ('progress_kind', constants.CHANGE_ROWS_PROGRESS_KIND),
            ('topic_name', table.topic_name)
        ))
        prior_snapshot_rows_progress = prior_progress.get((
            ('capture_instance_name', table.capture_instance_name),
            ('progress_kind', constants.SNAPSHOT_ROWS_PROGRESS_KIND),
            ('topic_name', table.topic_name)
        ))

        start_change_index = prior_change_rows_progress and tracked_tables.ChangeTableIndex(
            prior_change_rows_progress['last_published_change_table_lsn'],
            prior_change_rows_progress['last_published_change_table_seqval'],
            prior_change_rows_progress['last_published_change_table_operation']
        )
        start_snapshot_value = \
            prior_snapshot_rows_progress and \
            tuple(kf['value_as_string']
                  for kf in prior_snapshot_rows_progress['last_published_snapshot_key_field_values'])

        table.finalize_table(start_change_index or constants.BEGINNING_CHANGE_TABLE_INDEX, start_snapshot_value)

        if table.last_read_key_for_snapshot is None:
            snapshot_state = 'N/A'
        else:
            key_spec = ', '.join([f'{k}: {v}' for k, v in zip(table.key_field_names, table.last_read_key_for_snapshot)])
            snapshot_state = f'From {key_spec}'

        prior_progress_log_table_data.append((table.capture_instance_name, table.fq_name, table.topic_name,
                                              start_change_index or '<beginning>', snapshot_state))

    headers = ('Capture instance name', 'Source table name', 'Topic name', 'From change table index', 'Snapshots')
    table = tabulate(prior_progress_log_table_data, headers, tablefmt='fancy_grid')

    logger.info('Processing will proceed from the following positions based on the last message from each topic '
                'and/or the snapshot progress committed in Kafka (NB: snapshot reads occur BACKWARDS from high to '
                'low identity column values):\n%s', table)
