import datetime
import json
import logging
import queue
import re
import time
from typing import List

import pyodbc
from tabulate import tabulate

from . import clock_sync, kafka, tracked_tables, constants, options, validation, change_index
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


def run() -> None:
    opts, reporters = options.get_options_and_reporters()

    logger.debug('Parsed configuration: %s', json.dumps(vars(opts)))
    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.db_conn_string):
        raise Exception('Arguments schema_registry_url, kafka_bootstrap_servers, and db_conn_string are all required.')

    with get_db_conn(opts.db_conn_string) as db_conn:
        clock_syncer = clock_sync.ClockSync(db_conn)
        metrics_accumulator = accumulator.Accumulator(
            db_conn, clock_syncer, opts.metrics_namespace, opts.process_hostname)

        with kafka.KafkaClient(metrics_accumulator,
                               opts.kafka_bootstrap_servers,
                               opts.schema_registry_url,
                               opts.kafka_timeout_seconds,
                               opts.progress_topic_name,
                               tracked_tables.TrackedTable.capture_instance_name_resolver,
                               opts.extra_kafka_consumer_config,
                               opts.extra_kafka_producer_config) as kafka_client:

            tables = tracked_tables.build_tracked_tables_from_cdc_metadata(
                db_conn, clock_syncer, metrics_accumulator, opts.topic_name_template, opts.table_whitelist_regex,
                opts.table_blacklist_regex, opts.snapshot_table_whitelist_regex, opts.snapshot_table_blacklist_regex,
                opts.capture_instance_version_strategy, opts.capture_instance_version_regex)

            determine_start_points_and_finalize_tables(
                kafka_client, tables, opts.lsn_gap_handling, opts.partition_count, opts.replication_factor,
                opts.extra_topic_config, opts.progress_csv_path, opts.run_validations)

            if opts.run_validations:
                validator = validation.Validator(kafka_client, db_conn, tables)
                ok = validator.run()
                exit(ok)

            logger.info('Beginning processing for %s tracked table(s).', len(tables))

            # Prioritization in this queue is based on the commit time of the change data rows pulled from SQL. It will
            # hold one entry per tracked table, which will be replaced with the next entry for that table after the
            # current one is pulled off:
            pq = queue.PriorityQueue(len(tables))

            metrics_interval = datetime.timedelta(seconds=opts.metrics_reporting_interval)
            last_metrics_emission_time = datetime.datetime.utcnow()
            metrics_accumulator.reset_and_start()

            for table in tables:
                priority_tuple, msg_key, msg_value = table.pop_next()
                pq.put((priority_tuple, msg_key, msg_value, table))

            try:
                while True:

                    if (datetime.datetime.utcnow() - last_metrics_emission_time) > metrics_interval:
                        start_time = time.perf_counter()
                        metrics = metrics_accumulator.end_and_get_values()
                        for reporter in reporters:
                            try:
                                reporter.emit(metrics)  # TODO: async this
                            except Exception as e:
                                logger.exception('Caught exception while reporting metrics', e)
                        last_metrics_emission_time = datetime.datetime.utcnow()
                        metrics_accumulator.reset_and_start()
                        elapsed = (time.perf_counter() - start_time)
                        logger.debug('Metrics reporting completed in %s ms', elapsed * 1000)

                    priority_tuple, msg_key, msg_value, table = pq.get()
                    act_time = priority_tuple[0]
                    now = datetime.datetime.utcnow()
                    if act_time > now:
                        sleep_time = (act_time - now).total_seconds()
                        time.sleep(sleep_time)
                        metrics_accumulator.register_sleep(sleep_time)

                    if msg_key is not None:
                        kafka_client.produce(
                            table.topic_name, msg_key, table.key_schema_id, msg_value, table.value_schema_id)

                        if msg_value[constants.OPERATION_NAME] == constants.DELETE_OPERATION_NAME \
                                and not opts.disable_deletion_tombstones:
                            kafka_client.produce(
                                table.topic_name, msg_key, table.key_schema_id, None, table.value_schema_id)

                    kafka_client.commit_progress()

                    # Put the next entry for the table just handled back on the priority queue:
                    priority_tuple, msg_key, msg_value = table.pop_next()
                    pq.put((priority_tuple, msg_key, msg_value, table))
            except KeyboardInterrupt:
                logger.info('Exiting due to external interrupt.')
                exit(0)


def determine_start_points_and_finalize_tables(
        kafka_client: kafka.KafkaClient, tables: List[tracked_tables.TrackedTable], lsn_gap_handling: str,
        partition_count: int, replication_factor: int, extra_topic_config: str, progress_csv_path: str = None,
        validation_mode: bool = False) -> None:
    topic_names = [t.topic_name for t in tables]

    if validation_mode:
        for table in tables:
            table.snapshot_allowed = False
            table.finalize_table(change_index.BEGINNING_CHANGE_INDEX, {}, lsn_gap_handling,
                                 kafka_client.register_schemas)
        return

    watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
    first_check_watermarks_json = json.dumps(watermarks_by_topic, indent=4)

    logger.info('Pausing briefly to ensure target topics are not receiving new messages from elsewhere...')
    time.sleep(constants.STABLE_WATERMARK_CHECKS_DELAY_SECONDS)

    watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
    second_check_watermarks_json = json.dumps(watermarks_by_topic, indent=4)

    if first_check_watermarks_json != second_check_watermarks_json:
        raise Exception(f'Watermarks for one or more target topics changed between successive checks. '
                        f'Another process may be producing to the topic(s). Bailing.\nFirst check: '
                        f'{first_check_watermarks_json}\nSecond check: {second_check_watermarks_json}')

    prior_progress = kafka_client.get_prior_progress_or_create_progress_topic(progress_csv_path)
    prior_progress_log_table_data = []
    created_topics = False

    for table in tables:
        if table.topic_name not in watermarks_by_topic:
            if partition_count:
                this_topic_partition_count = partition_count
            else:
                per_second = table.get_change_rows_per_second()
                # one partition per 500K rows/day on average:
                this_topic_partition_count = max(2, int(per_second * 60 * 60 * 24 / 500_000))
                if this_topic_partition_count > 100:
                    raise Exception(
                        f'Automatic topic creation would create %{this_topic_partition_count} partitions for topic '
                        f'{table.topic_name} based on a change table rows per second rate of {per_second}. This '
                        f'seems excessive, so the program is exiting to prevent overwhelming your Kafka cluster. '
                        f'Look at setting PARTITION_COUNT to take manual control of this.')
            logger.info('Creating topic %s with %s partitions', table.topic_name, this_topic_partition_count)
            kafka_client.create_topic(table.topic_name, this_topic_partition_count, replication_factor,
                                      extra_topic_config)
            created_topics = True
            start_change_index, start_snapshot_value = None, None
        else:
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

            start_change_index = prior_change_rows_progress and change_index.ChangeIndex(
                prior_change_rows_progress['last_ack_change_table_lsn'],
                prior_change_rows_progress['last_ack_change_table_seqval'],
                prior_change_rows_progress['last_ack_change_table_operation']
            )
            start_snapshot_value = prior_snapshot_rows_progress and \
                prior_snapshot_rows_progress['last_ack_snapshot_key_field_values']

        table.finalize_table(start_change_index or change_index.BEGINNING_CHANGE_INDEX, start_snapshot_value,
                             lsn_gap_handling, kafka_client.register_schemas, kafka_client.reset_progress)

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

    if created_topics:
        time.sleep(5)
        kafka_client.refresh_cluster_metadata()

    headers = ('Capture instance name', 'Source table name', 'Topic name', 'From change table index', 'Snapshots')
    table = tabulate(prior_progress_log_table_data, headers, tablefmt='fancy_grid')

    logger.info('Processing will proceed from the following positions based on the last message from each topic '
                'and/or the snapshot progress committed in Kafka (NB: snapshot reads occur BACKWARDS from high to '
                'low key column values):\n%s', table)


def get_db_conn(odbc_conn_string: str) -> pyodbc.Connection:
    # The Linux ODBC driver doesn't do failover, so we're hacking it in here. This will only work for initial
    # connections. If a failover happens while this process is running, the app will crash. Have a process supervisor
    # that can restart it if that happens, and it'll connect to the failover on restart:
    # THIS ASSUMES that you are using the exact keywords 'SERVER' and 'Failover_Partner' in your connection string!
    try:
        return pyodbc.connect(odbc_conn_string)
    except pyodbc.ProgrammingError as e:
        if e.args[0] != '42000':
            raise
        server = re.match(r".*SERVER=(?P<hostname>.*?);", odbc_conn_string)
        failover_partner = re.match(r".*Failover_Partner=(?P<hostname>.*?);", odbc_conn_string)
        if failover_partner is not None and server is not None:
            failover_partner = failover_partner.groups('hostname')[0]
            server = server.groups('hostname')[0]
            odbc_conn_string = odbc_conn_string.replace(server, failover_partner)
            return pyodbc.connect(odbc_conn_string)
