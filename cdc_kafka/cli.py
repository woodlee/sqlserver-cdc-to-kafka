import argparse
import datetime
import importlib
import json
import logging
import os
import queue
import time
from typing import List

import pyodbc
from tabulate import tabulate

from cdc_kafka import validation
from . import kafka, tracked_tables, constants, metric_reporters

logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()

    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

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
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG'),
                   help="Additional config parameters to be used when instantiating the Kafka consumer (used only for "
                        "checking saved progress upon startup, and when in validation mode). Should be a "
                        "semicolon-separated list of colon-delimited librdkafka config key:value pairs, e.g. "
                        "'queued.max.messages.kbytes:500000;fetch.wait.max.ms:250'")

    p.add_argument('--extra-kafka-producer-config',
                   default=os.environ.get('EXTRA_KAFKA_PRODUCER_CONFIG'),
                   help="Additional config parameters to be used when instantiating the Kafka producer. Should be a "
                        "semicolon-separated list of colon-delimited librdkafka config key:value pairs, e.g. "
                        "'linger.ms:200;retry.backoff.ms:250'")

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
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('DISABLE_DELETION_TOMBSTONES', False)),
                   help="Name of the topic used to store progress details reading change tables (and also source "
                        "tables, in the case of snapshots). This process will create the topic if it does not yet "
                        "exist. It should have only one partition.")

    p.add_argument('--lsn-gap-handling',
                   choices=('raise_exception', 'begin_new_snapshot', 'ignore'),
                   default=os.environ.get('LSN_GAP_HANDLING', 'raise_exception'),
                   help="Controls what happens if the earliest available change LSN in a capture instance is after "
                        "the LSN of the latest change published to Kafka.")

    p.add_argument('--run-validations',
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('RUN_VALIDATIONS', False)),
                   help="Runs count validations between messages in the Kafka topic and rows in the change and"
                        "source tables, then quits. Respects the table whitelist/blacklist regexes.")

    p.add_argument('--metric-reporters',
                   default=os.environ.get('METRIC_REPORTERS', 'cdc_kafka.metric_reporters.stdout.StdoutReporter'),
                   help="Comma-separated list of <module_name>.<class_name>s of metric reporters you want this app "
                        "to emit to.")

    p.add_argument('--metric-reporting-interval',
                   type=int,
                   default=os.environ.get('METRIC_REPORTING_INTERVAL', 15),
                   help="Interval in seconds between calls to metric reporters.")

    p.add_argument('--partition-count',
                   type=int,
                   default=os.environ.get('PARTITION_COUNT', 12),
                   help="Number of partitions to specify when creating new topics")

    p.add_argument('--replication-factor',
                   type=int,
                   default=os.environ.get('REPLICATION_FACTOR'),
                   help="Replication factor to specify when creating new topics")

    p.add_argument('--extra-topic-config',
                   default=os.environ.get('EXTRA_TOPIC_CONFIG'),
                   help="Additional config parameters to be used when creating new topics. Should be a "
                        "semicolon-separated list of colon-delimited librdkafka config key:value pairs, e.g. "
                        "'min.insync.replicas:2'")

    opts = p.parse_args()

    reporters = []
    if opts.metric_reporters:
        for class_path in opts.metric_reporters.split(','):
            package_module, class_name = class_path.rsplit('.', 1)
            module = importlib.import_module(package_module)
            reporter = getattr(module, class_name)()
            reporters.append(reporter)
            reporter.add_arguments(p)

        opts = p.parse_args()

        for reporter in reporters:
            reporter.set_options(opts)

    logger.debug('Parsed configuration: \n%s', json.dumps(vars(opts), indent=4))
    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.db_conn_string):
        raise Exception('Arguments schema_registry_url, kafka_bootstrap_servers, and db_conn_string are all required.')

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

        determine_start_points_and_finalize_tables(
            kafka_client, tables, opts.lsn_gap_handling, opts.partition_count, opts.replication_factor,
            opts.extra_topic_config, opts.run_validations)

        if opts.run_validations:
            validator = validation.Validator(kafka_client, db_conn, tables)
            ok = validator.run()
            exit(ok)

        logger.info('Beginning processing for %s tracked tables.', len(tables))

        # Prioritization in this queue is based on the commit time of the change data rows pulled from SQL. It will
        # hold one entry per tracked table, which will be replaced with the next entry for that table after the
        # current one is pulled off:
        pq = queue.PriorityQueue(len(tables))

        for table in tables:
            priority_tuple, msg_key, msg_value = table.pop_next()
            pq.put((priority_tuple, msg_key, msg_value, table))

        metrics_interval = datetime.timedelta(seconds=opts.metric_reporting_interval)
        last_metrics_emission_time = datetime.datetime.now()
        last_published_msg_db_time = None
        metrics_accum = metric_reporters.MetricsAccumulator(db_conn)
        loop_entry_time = time.perf_counter()
        pop_time_total, produce_time_total, publish_count = 0.0, 0.0, 0

        try:

            while True:
                if (datetime.datetime.now() - last_metrics_emission_time) > metrics_interval:
                    metrics_accum.determine_lags(last_published_msg_db_time)
                    for reporter in reporters:
                        reporter.emit(metrics_accum)
                    last_metrics_emission_time = datetime.datetime.now()
                    metrics_accum = metric_reporters.MetricsAccumulator(db_conn)
                    total_time = time.perf_counter() - loop_entry_time
                    logger.debug('Timings per msg: overall - %s us, DB (pop) - %s us, Kafka (produce/commit) - %s us',
                                 total_time / publish_count * 1000000, pop_time_total / publish_count * 1000000,
                                 produce_time_total / publish_count * 1000000)

                priority_tuple, msg_key, msg_value, table = pq.get()

                if msg_key is not None:
                    start_time = time.perf_counter()
                    kafka_client.produce(
                        table.topic_name, msg_key, table.key_schema_id, msg_value, table.value_schema_id)
                    produce_time_total += (time.perf_counter() - start_time)
                    publish_count += 1
                    metrics_accum.record_publish += 1
                    last_published_msg_db_time = priority_tuple[0]

                    if msg_value[constants.OPERATION_NAME] == 'Delete' and not opts.disable_deletion_tombstones:
                        start_time = time.perf_counter()
                        kafka_client.produce(
                            table.topic_name, msg_key, table.key_schema_id, None, table.value_schema_id)
                        produce_time_total += (time.perf_counter() - start_time)
                        publish_count += 1
                        metrics_accum.tombstone_publish += 1

                # Put the next entry for the table just handled back on the priority queue:
                start_time = time.perf_counter()
                priority_tuple, msg_key, msg_value = table.pop_next()
                pop_time_total += (time.perf_counter() - start_time)
                pq.put((priority_tuple, msg_key, msg_value, table))
        except KeyboardInterrupt:
            logger.info('Exiting due to external interrupt.')
            exit(0)


def determine_start_points_and_finalize_tables(
        kafka_client: kafka.KafkaClient, tables: List[tracked_tables.TrackedTable], lsn_gap_handling: str,
        partition_count: int, replication_factor: int, extra_topic_config: str, validation_mode: bool = False) -> None:
    topic_names = [t.topic_name for t in tables]

    if validation_mode:
        for table in tables:
            table.snapshot_allowed = False
            table.finalize_table(constants.BEGINNING_CHANGE_TABLE_INDEX, (None,), lsn_gap_handling, None)
        return

    # watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
    # first_check_watermarks_json = json.dumps(watermarks_by_topic, indent=4)
    #
    # logger.info('Pausing briefly to ensure target topics are not receiving new messages from elsewhere...')
    # time.sleep(constants.STABLE_WATERMARK_CHECKS_INTERVAL_SECONDS)
    #
    # watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
    # second_check_watermarks_json = json.dumps(watermarks_by_topic, indent=4)
    #
    # if first_check_watermarks_json != second_check_watermarks_json:
    #     raise Exception(f'Watermarks for one or more target topics changed between successive checks. '
    #                     f'Another process may be producing to the topic(s). Bailing.\nFirst check: '
    #                     f'{first_check_watermarks_json}\nSecond check: {second_check_watermarks_json}')

    prior_progress = kafka_client.get_prior_progress_or_create_progress_topic()
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

        table.finalize_table(start_change_index or constants.BEGINNING_CHANGE_TABLE_INDEX,
                             start_snapshot_value, lsn_gap_handling, kafka_client.register_schemas)

        # if table.topic_name not in watermarks_by_topic:
        #     logger.info('Creating topic %s', table.topic_name)
        #     kafka_client.create_topic(table.topic_name, partition_count, replication_factor, extra_topic_config)

        if not table.snapshot_allowed:
            snapshot_state = '<not doing>'
        elif table.snapshot_complete:
            snapshot_state = '<already complete>'
        elif table.last_read_key_for_snapshot_display is None:
            snapshot_state = '<from beginning>'
        else:
            snapshot_state = f'From {table.last_read_key_for_snapshot_display}'

        prior_progress_log_table_data.append((table.capture_instance_name, table.fq_name, table.topic_name,
                                              start_change_index or '<from beginning>', snapshot_state))

    headers = ('Capture instance name', 'Source table name', 'Topic name', 'From change table index', 'Snapshots')
    table = tabulate(prior_progress_log_table_data, headers, tablefmt='fancy_grid')

    logger.info('Processing will proceed from the following positions based on the last message from each topic '
                'and/or the snapshot progress committed in Kafka (NB: snapshot reads occur BACKWARDS from high to '
                'low key column values):\n%s', table)
