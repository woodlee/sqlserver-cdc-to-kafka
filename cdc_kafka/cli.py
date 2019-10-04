import argparse
import datetime
import json
import logging
import os
import queue
import time
from typing import List

import sqlalchemy

from . import kafka, tracked_tables, constants

logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()

    # Required
    p.add_argument('--db-conn-string',
                   default=os.environ.get('DB_CONN_STRING'),
                   help='URL-form connection string for the DB from which you wish to consume CDC logs (see '
                        'https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls)')

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

    opts = p.parse_args()

    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.db_conn_string):
        raise Exception('Arguments schema_registry_url, kafka_bootstrap_servers, and db_conn_string are all required.')
        
    logger.debug('Parsed configuration: \n%s', json.dumps(vars(opts), indent=4))

    with sqlalchemy.create_engine(opts.db_conn_string).connect() as db_conn, \
            kafka.KafkaClient(opts.kafka_bootstrap_servers, opts.schema_registry_url, opts.kafka_timeout_seconds,
                              opts.extra_kafka_consumer_config, opts.extra_kafka_producer_config) as kafka_client:

        tables = tracked_tables.build_tracked_tables_from_cdc_metadata(
            db_conn, opts.topic_name_template, opts.table_whitelist_regex, opts.table_blacklist_regex,
            opts.snapshot_table_whitelist_regex, opts.snapshot_table_blacklist_regex,
            opts.capture_instance_version_strategy, opts.capture_instance_version_regex)

        determine_start_points_and_finalize_tables(kafka_client, tables)

        logger.info('Beginning processing for %s tracked tables.', len(tables))

        # Prioritization in this queue is based on the commit time of the change data rows pulled from SQL. It will hold
        # one item per tracked table, which will be replaced with the next item for that table after it's pulled off.
        pq = queue.PriorityQueue(len(tables))

        for table in tables:
            change_time, change_index, msg_key, msg_value = table.pop_next() or (
                datetime.datetime.now() + constants.TABLE_POLL_INTERVAL,
                constants.BEGINNING_CHANGE_TABLE_INDEX, None, None)
            pq.put((change_time, change_index, msg_key, msg_value, table))

        last_log_time = datetime.datetime.now()
        published_ctr = 0

        while True:
            if (datetime.datetime.now() - last_log_time) > datetime.timedelta(minutes=1):
                logger.info('Published %s change rows in the past minute.', published_ctr)
                last_log_time = datetime.datetime.now()
                published_ctr = 0

            change_time, change_index, msg_key, msg_value, table = pq.get()

            if msg_key is not None:
                kafka_client.produce(table.topic_name, msg_key, table.key_schema, msg_value, table.value_schema)
                published_ctr += 1

            # Put the next entry for the table back on the queue:
            change_time, change_index, msg_key, msg_value = table.pop_next() or (
                datetime.datetime.now() + constants.TABLE_POLL_INTERVAL,
                constants.BEGINNING_CHANGE_TABLE_INDEX, None, None)
            pq.put((change_time, change_index, msg_key, msg_value, table))


def determine_start_points_and_finalize_tables(kafka_client: kafka.KafkaClient,
                                               tables: List[tracked_tables.TrackedTable]) -> None:
    topic_names = [t.topic_name for t in tables]

    last_messages_by_topic = kafka_client.get_last_messages_for_topics(topic_names)

    logger.info('Pausing briefly to ensure target topics are not receiving new messages from elsewhere...')
    time.sleep(5)

    rechecked_last_messages = kafka_client.get_last_messages_for_topics(topic_names)

    for topic_name, rechecked_messages in rechecked_last_messages.items():
        rechecked_offsets = {(msg.partition(), msg.offset()) for msg in rechecked_messages}
        original_offsets = {(msg.partition(), msg.offset()) for msg in last_messages_by_topic[topic_name]}

        if rechecked_offsets != original_offsets:
            raise Exception(f'Topic {topic_name}: offsets of final messages moved between two successive checks. '
                            f'Another process may be producing to the topic. Bailing.\nFirst check: '
                            f'{original_offsets}\nSecond check: {rechecked_offsets}')

    logger.info('Processing will pick up from these values obtained from the last message in each topic:')

    for table in tables:
        if table.topic_name in last_messages_by_topic:
            msgs = last_messages_by_topic[table.topic_name]

            lasts = [(
                tracked_tables.ChangeTableIndex(msg.value()['_cdc_start_lsn'],
                                                msg.value()['_cdc_seqval'],
                                                msg.value()['_cdc_operation']),
                msg.value()['_cdc_tran_end_time']
            ) for msg in msgs]

            last_published_change_table_index, last_published_change_time = sorted(lasts, key=lambda x: x[0])[-1]
        else:
            last_published_change_table_index, last_published_change_time = \
                constants.BEGINNING_CHANGE_TABLE_INDEX, constants.BEGINNING_DATETIME

        table.finalize_table(last_published_change_table_index, last_published_change_time)

        logger.info('Capture instance %s: change table index %s, change time %s', table.capture_instance_name,
                    last_published_change_table_index, last_published_change_time)
