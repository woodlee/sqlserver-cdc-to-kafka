import argparse
import collections
import datetime
import heapq
import json
import logging
import re
import time
from typing import Dict, Optional, List, Any, Iterable, Union, Tuple

import pyodbc
from tabulate import tabulate

from . import clock_sync, kafka, tracked_tables, constants, options, validation, change_index, progress_tracking, \
    sql_query_subprocess, sql_queries
from .metric_reporting import accumulator

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from . import parsed_row

logger = logging.getLogger(__name__)


def run() -> None:
    opts: argparse.Namespace
    opts, reporters = options.get_options_and_metrics_reporters()

    logger.debug('Parsed configuration: %s', json.dumps(vars(opts)))

    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.db_conn_string):
        raise Exception('Arguments schema_registry_url, kafka_bootstrap_servers, and db_conn_string are all required.')

    redo_snapshot_for_new_instance: bool = \
        opts.new_capture_instance_snapshot_handling == options.NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_BEGIN_NEW
    publish_duplicate_changes_from_new_instance: bool = \
        opts.new_capture_instance_overlap_handling == options.NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_REPUBLISH

    with sql_query_subprocess.get_db_conn(
        opts.db_conn_string
    ) as db_conn, sql_query_subprocess.SQLQueryProcessor(
        opts.db_conn_string
    ) as sql_query_processor:
        clock_syncer: clock_sync.ClockSync = clock_sync.ClockSync(db_conn)

        metrics_accumulator: accumulator.Accumulator = accumulator.Accumulator(
            db_conn, clock_syncer, opts.metrics_namespace, opts.process_hostname)

        capture_instances_by_fq_name: Dict[str, Dict[str, Any]] = get_latest_capture_instances_by_fq_name(
            db_conn, opts.capture_instance_version_strategy, opts.capture_instance_version_regex,
            opts.table_whitelist_regex, opts.table_blacklist_regex)

        if not capture_instances_by_fq_name:
            logger.error('No capture instances could be found.')
            exit(1)

        capture_instance_names: List[str] = [ci['capture_instance_name']
                                             for ci in capture_instances_by_fq_name.values()]

        tables: List[tracked_tables.TrackedTable] = build_tracked_tables_from_cdc_metadata(
            db_conn, clock_syncer, metrics_accumulator, opts.topic_name_template, opts.snapshot_table_whitelist_regex,
            opts.snapshot_table_blacklist_regex, opts.truncate_fields, capture_instance_names, sql_query_processor)

        topic_to_source_table_map: Dict[str, str] = {
            t.topic_name: t.fq_name for t in tables}
        topic_to_change_table_map: Dict[str, str] = {
            t.topic_name: f'{constants.CDC_DB_SCHEMA_NAME}.{t.change_table_name}' for t in tables}
        capture_instance_to_topic_map: Dict[str, str] = {
            t.capture_instance_name: t.topic_name for t in tables}

        with kafka.KafkaClient(
            metrics_accumulator, opts.kafka_bootstrap_servers, opts.schema_registry_url,
            opts.extra_kafka_consumer_config, opts.extra_kafka_producer_config,
            disable_writing=opts.run_validations or opts.report_progress_only
        ) as kafka_client, progress_tracking.ProgressTracker(
            kafka_client, opts.progress_topic_name, topic_to_source_table_map, topic_to_change_table_map
        ) as progress_tracker:

            kafka_client.register_delivery_callback((
                constants.SINGLE_TABLE_CHANGE_MESSAGE, constants.SINGLE_TABLE_SNAPSHOT_MESSAGE
            ), progress_tracker.kafka_delivery_callback)
            kafka_client.register_delivery_callback((
                constants.SINGLE_TABLE_CHANGE_MESSAGE, constants.UNIFIED_TOPIC_CHANGE_MESSAGE,
                constants.SINGLE_TABLE_SNAPSHOT_MESSAGE, constants.DELETION_CHANGE_TOMBSTONE_MESSAGE
            ), metrics_accumulator.kafka_delivery_callback)

            determine_start_points_and_finalize_tables(
                kafka_client, tables, progress_tracker, opts.lsn_gap_handling, opts.partition_count,
                opts.replication_factor, opts.extra_topic_config, opts.run_validations, redo_snapshot_for_new_instance,
                publish_duplicate_changes_from_new_instance, opts.report_progress_only)

            if opts.report_progress_only:
                exit(0)

            table_to_unified_topics_map: Dict[str, List[Tuple[str]]] = collections.defaultdict(list)
            unified_topic_to_tables_map: Dict[str, List[tracked_tables.TrackedTable]] = collections.defaultdict(list)

            # "Unified" topics contain change messages from multiple capture instances in a globally-consistent LSN
            # order. They don't contain snapshot messages.
            if opts.unified_topics:
                for unified_topic_name, unified_topic_config in opts.unified_topics.items():
                    included_tables_regex = unified_topic_config['included_tables']
                    compiled_regex = re.compile(included_tables_regex, re.IGNORECASE)
                    matched_tables = [table for table in tables if compiled_regex.match(table.fq_name)]
                    if matched_tables:
                        for matched_table in matched_tables:
                            table_to_unified_topics_map[matched_table.fq_name].append(unified_topic_name)
                            unified_topic_to_tables_map[unified_topic_name].append(matched_table)
                        part_count = kafka_client.get_topic_partition_count(unified_topic_name)
                        if part_count:
                            logger.info('Existing unified topic %s found, with %s partition(s)',
                                        unified_topic_name, part_count)
                        else:
                            part_count = unified_topic_config.get('partition_count', 1)
                            extra_config = unified_topic_config.get('extra_topic_config', {})
                            logger.info('Unified topic %s not found, creating with %s replicas, %s partition(s), and '
                                        'extra config %s', unified_topic_name, opts.replication_factor, part_count,
                                        extra_config)
                            kafka_client.create_topic(unified_topic_name, part_count, opts.replication_factor,
                                                      extra_config)

            if table_to_unified_topics_map:
                logger.debug('Unified topics being produced to, by table: %s', table_to_unified_topics_map)

            # Validations will go through all messages in all topics and try to warn of any inconsistencies between
            # those and the source DB data. It takes a while; probably don't run this on very large datasets!
            if opts.run_validations:
                validator: validation.Validator = validation.Validator(
                    kafka_client, tables, progress_tracker, unified_topic_to_tables_map)
                validator.run()
                exit(0)

            last_metrics_emission_time: datetime.datetime = datetime.datetime.utcnow()
            last_capture_instance_check_time: datetime.datetime = datetime.datetime.utcnow()
            last_slow_table_heartbeat_time: datetime.datetime = datetime.datetime.utcnow()
            next_cdc_poll_allowed_time: datetime.datetime = datetime.datetime.utcnow()
            next_cdc_poll_due_time: datetime.datetime = datetime.datetime.utcnow()
            last_produced_row: Optional['parsed_row.ParsedRow'] = None
            last_topic_produces: Dict[str, datetime.datetime] = {}
            change_rows_queue: List[Tuple[change_index.ChangeIndex, 'parsed_row.ParsedRow']] = []
            queued_change_row_counts: Dict[str, int] = {t.topic_name: 0 for t in tables}

            # Returned bool indicates whether the process should halt
            def poll_periodic_tasks() -> bool:
                nonlocal last_metrics_emission_time
                nonlocal last_slow_table_heartbeat_time
                nonlocal last_capture_instance_check_time

                if (datetime.datetime.utcnow() - last_metrics_emission_time) > constants.METRICS_REPORTING_INTERVAL:
                    start_time = time.perf_counter()
                    metrics = metrics_accumulator.end_and_get_values()
                    for reporter in reporters:
                        try:
                            reporter.emit(metrics)  # TODO: async this
                        except Exception as e:
                            logger.exception('Caught exception while reporting metrics', exc_info=e)
                    elapsed = (time.perf_counter() - start_time)
                    logger.debug('Metrics reporting completed in %s ms', elapsed * 1000)
                    metrics_accumulator.reset_and_start()
                    last_metrics_emission_time = datetime.datetime.utcnow()

                if (datetime.datetime.utcnow() - last_slow_table_heartbeat_time) > \
                        constants.SLOW_TABLE_PROGRESS_HEARTBEAT_INTERVAL:
                    for t in tables:
                        if queued_change_row_counts[t.topic_name] == 0:
                            last_topic_produce = last_topic_produces.get(t.topic_name)
                            if not last_topic_produce or (datetime.datetime.utcnow() - last_topic_produce) > \
                                    2 * constants.SLOW_TABLE_PROGRESS_HEARTBEAT_INTERVAL:
                                logger.debug('Emitting heartbeat progress for slow table %s', t.fq_name)
                                progress_tracker.emit_changes_progress_heartbeat(
                                    t.topic_name, t.max_polled_change_index)
                    last_slow_table_heartbeat_time = datetime.datetime.utcnow()

                if opts.terminate_on_capture_instance_change and \
                        (datetime.datetime.utcnow() - last_capture_instance_check_time) > \
                        constants.CHANGED_CAPTURE_INSTANCES_CHECK_INTERVAL:
                    topic_to_max_polled_index_map:  Dict[str, change_index.ChangeIndex] = {
                        t.topic_name: t.max_polled_change_index for t in tables
                    }
                    if should_terminate_due_to_capture_instance_change(
                            db_conn, progress_tracker, opts.capture_instance_version_strategy,
                            opts.capture_instance_version_regex, capture_instance_to_topic_map,
                            capture_instances_by_fq_name, opts.table_whitelist_regex,
                            opts.table_blacklist_regex, topic_to_max_polled_index_map):
                        return True
                    last_capture_instance_check_time = datetime.datetime.utcnow()
                return False

            logger.info('Beginning processing for %s tracked table(s).', len(tables))
            metrics_accumulator.reset_and_start()

            # The above is all setup, now we come to the "hot loop":

            try:
                while True:
                    snapshots_remain: bool = not all([t.snapshot_complete for t in tables])
                    change_tables_lagging: bool = any([t.change_reads_are_lagging for t in tables])

                    # ----- Poll for and produce snapshot data while change row queries run -----

                    if snapshots_remain and not change_tables_lagging:
                        while datetime.datetime.utcnow() < next_cdc_poll_due_time:
                            for t in tables:
                                if not t.snapshot_complete:
                                    for row in t.retrieve_snapshot_query_results():
                                        kafka_client.produce(row.destination_topic, row.key_dict,
                                                             row.avro_key_schema_id, row.value_dict,
                                                             row.avro_value_schema_id,
                                                             constants.SINGLE_TABLE_SNAPSHOT_MESSAGE)
                                    if t.snapshot_complete:
                                        progress_tracker.record_snapshot_completion(row.destination_topic)
                                t.enqueue_snapshot_query()   # NB: results may not be retrieved until next cycle
                                if datetime.datetime.utcnow() > next_cdc_poll_due_time:
                                    break
                        if poll_periodic_tasks():
                            break

                    # ----- Wait for next poll window (if needed) and get ceiling LSN for cycle -----

                    if not change_tables_lagging:
                        wait_time = (next_cdc_poll_allowed_time - datetime.datetime.utcnow()).total_seconds()
                        if wait_time > 0:
                            time.sleep(wait_time)
                            metrics_accumulator.register_sleep(wait_time)

                    if poll_periodic_tasks():
                        break

                    with db_conn.cursor() as cursor:
                        q, _ = sql_queries.get_max_lsn()
                        cursor.execute(q)
                        lsn_limit = cursor.fetchval()

                    next_cdc_poll_allowed_time = (datetime.datetime.utcnow() + constants.MIN_CDC_POLLING_INTERVAL)
                    next_cdc_poll_due_time = (datetime.datetime.utcnow() + constants.MAX_CDC_POLLING_INTERVAL)

                    # ----- Query for change rows ----

                    for t in tables:
                        if queued_change_row_counts[t.topic_name] < constants.DB_ROW_BATCH_SIZE + 1:
                            t.enqueue_changes_query(lsn_limit)

                    common_lsn_limit: change_index.ChangeIndex = change_index.HIGHEST_CHANGE_INDEX

                    if poll_periodic_tasks():
                        break

                    for t in tables:
                        for row in t.retrieve_changes_query_results():
                            queued_change_row_counts[t.topic_name] += 1
                            heapq.heappush(change_rows_queue, (row.change_idx, row))
                        if t.max_polled_change_index < common_lsn_limit:
                            common_lsn_limit = t.max_polled_change_index

                    if poll_periodic_tasks():
                        break

                    # ----- Produce change data to Kafka and commit progress -----

                    while change_rows_queue:
                        row: 'parsed_row.ParsedRow' = heapq.heappop(change_rows_queue)[1]

                        if row.change_idx > common_lsn_limit:
                            heapq.heappush(change_rows_queue, (row.change_idx, row))
                            break

                        if last_produced_row and row.change_idx < last_produced_row.change_idx:
                            raise Exception(f'Change rows are being produced to Kafka out of LSN order. There is '
                                            f'a bug. Fix it! Prior: {last_produced_row}, current: {row}')
                        last_produced_row = row
                        queued_change_row_counts[row.destination_topic] -= 1

                        kafka_client.produce(row.destination_topic, row.key_dict, row.avro_key_schema_id,
                                             row.value_dict, row.avro_value_schema_id,
                                             constants.SINGLE_TABLE_CHANGE_MESSAGE,
                                             table_to_unified_topics_map.get(row.table_fq_name, []))
                        last_topic_produces[row.destination_topic] = datetime.datetime.utcnow()

                        if not opts.disable_deletion_tombstones and row.operation_name == \
                                constants.DELETE_OPERATION_NAME:
                            kafka_client.produce(row.destination_topic, row.key_dict, row.avro_key_schema_id,
                                                 None, row.avro_value_schema_id,
                                                 constants.DELETION_CHANGE_TOMBSTONE_MESSAGE)

                    progress_tracker.commit_progress()

                    if poll_periodic_tasks():
                        break
            except (KeyboardInterrupt, pyodbc.OperationalError):
                logger.info('Exiting due to external interrupt.')


# This pulls the "greatest" capture instance running for each source table, in the event there is more than one.
def get_latest_capture_instances_by_fq_name(
        db_conn: pyodbc.Connection, capture_instance_version_strategy: str, capture_instance_version_regex: str,
        table_whitelist_regex: str, table_blacklist_regex: str
) -> Dict[str, Dict[str, Any]]:
    if capture_instance_version_strategy == options.CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX \
            and not capture_instance_version_regex:
        raise Exception('Please provide a capture_instance_version_regex when specifying the `regex` '
                        'capture_instance_version_strategy.')
    result: Dict[str, Dict[str, Any]] = {}
    fq_name_to_capture_instances: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
    capture_instance_version_regex = capture_instance_version_regex and re.compile(capture_instance_version_regex)
    table_whitelist_regex = table_whitelist_regex and re.compile(table_whitelist_regex, re.IGNORECASE)
    table_blacklist_regex = table_blacklist_regex and re.compile(table_blacklist_regex, re.IGNORECASE)

    with db_conn.cursor() as cursor:
        q, p = sql_queries.get_cdc_capture_instances_metadata()
        cursor.execute(q)
        for row in cursor.fetchall():
            fq_table_name = f'{row[0]}.{row[1]}'

            if table_whitelist_regex and not table_whitelist_regex.match(fq_table_name):
                logger.debug('Table %s excluded by whitelist', fq_table_name)
                continue

            if table_blacklist_regex and table_blacklist_regex.match(fq_table_name):
                logger.debug('Table %s excluded by blacklist', fq_table_name)
                continue

            if row[3] is None or row[4] is None:
                logger.debug('Capture instance for %s appears to be brand-new; will evaluate again on '
                             'next pass', fq_table_name)
                continue

            as_dict = {
                'fq_name': fq_table_name,
                'capture_instance_name': row[2],
                'start_lsn': row[3],
                'create_date': row[4],
            }
            if capture_instance_version_regex:
                match = capture_instance_version_regex.match(row[1])
                as_dict['regex_matched_group'] = match and match.group(1) or ''
            fq_name_to_capture_instances[as_dict['fq_name']].append(as_dict)

    for fq_name, capture_instances in fq_name_to_capture_instances.items():
        if capture_instance_version_strategy == options.CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE:
            latest_instance = sorted(capture_instances, key=lambda x: x['create_date'])[-1]
        elif capture_instance_version_strategy == options.CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX:
            latest_instance = sorted(capture_instances, key=lambda x: x['regex_matched_group'])[-1]
        else:
            raise Exception(f'Capture instance version strategy "{capture_instance_version_strategy}" not recognized.')
        result[fq_name] = latest_instance

    logger.debug('Latest capture instance names determined by "%s" strategy: %s', capture_instance_version_strategy,
                 sorted([v['capture_instance_name'] for v in result.values()]))

    return result


def build_tracked_tables_from_cdc_metadata(
    db_conn: pyodbc.Connection, clock_syncer: 'clock_sync.ClockSync', metrics_accumulator: 'accumulator.Accumulator',
    topic_name_template: str, snapshot_table_whitelist_regex: str, snapshot_table_blacklist_regex: str,
    truncate_fields: Dict[str, int], capture_instance_names: List[str],
    sql_query_processor: 'sql_query_subprocess.SQLQueryProcessor'
) -> List[tracked_tables.TrackedTable]:
    result: List[tracked_tables.TrackedTable] = []

    truncate_fields = {k.lower(): v for k, v in truncate_fields.items()}

    snapshot_table_whitelist_regex = snapshot_table_whitelist_regex and re.compile(
        snapshot_table_whitelist_regex, re.IGNORECASE)
    snapshot_table_blacklist_regex = snapshot_table_blacklist_regex and re.compile(
        snapshot_table_blacklist_regex, re.IGNORECASE)

    name_to_meta_fields: Dict[Tuple, List[Tuple]] = collections.defaultdict(list)

    with db_conn.cursor() as cursor:
        q, p = sql_queries.get_cdc_tracked_tables_metadata(capture_instance_names)
        cursor.execute(q)
        for row in cursor.fetchall():
            # 0:4 gets schema name, table name, capture instance name, min captured LSN:
            name_to_meta_fields[tuple(row[0:4])].append(row[4:])

    for (schema_name, table_name, capture_instance_name, min_lsn), fields in name_to_meta_fields.items():
        fq_table_name = f'{schema_name}.{table_name}'

        can_snapshot = False

        if snapshot_table_whitelist_regex and snapshot_table_whitelist_regex.match(fq_table_name):
            logger.debug('Table %s matched snapshotting whitelist', fq_table_name)
            can_snapshot = True

        if snapshot_table_blacklist_regex and snapshot_table_blacklist_regex.match(fq_table_name):
            logger.debug('Table %s matched snapshotting blacklist and will NOT be snapshotted', fq_table_name)
            can_snapshot = False

        topic_name = topic_name_template.format(
            schema_name=schema_name, table_name=table_name, capture_instance_name=capture_instance_name)

        tracked_table = tracked_tables.TrackedTable(
            db_conn, clock_syncer, metrics_accumulator, sql_query_processor, schema_name, table_name,
            capture_instance_name, topic_name, min_lsn, can_snapshot)

        for (change_table_ordinal, column_name, sql_type_name, primary_key_ordinal, decimal_precision,
             decimal_scale) in fields:
            truncate_after = truncate_fields.get(f'{schema_name}.{table_name}.{column_name}'.lower())
            tracked_table.append_field(tracked_tables.TrackedField(
                column_name, sql_type_name, change_table_ordinal, primary_key_ordinal, decimal_precision,
                decimal_scale, truncate_after))

        result.append(tracked_table)

    return result


def determine_start_points_and_finalize_tables(
        kafka_client: kafka.KafkaClient, tables: Iterable[tracked_tables.TrackedTable],
        progress_tracker: progress_tracking.ProgressTracker, lsn_gap_handling: str,
        partition_count: int, replication_factor: int, extra_topic_config: Dict[str, Union[str, int]],
        validation_mode: bool = False, redo_snapshot_for_new_instance: bool = False,
        publish_duplicate_changes_from_new_instance: bool = False, report_progress_only: bool = False
) -> None:
    topic_names: List[str] = [t.topic_name for t in tables]

    if validation_mode:
        for table in tables:
            table.snapshot_allowed = False
            table.finalize_table(change_index.LOWEST_CHANGE_INDEX, {}, lsn_gap_handling, kafka_client.register_schemas)
        return

    if report_progress_only:
        watermarks_by_topic = []
    else:
        watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
        first_check_watermarks_json = json.dumps(watermarks_by_topic)

        logger.info('Pausing briefly to ensure target topics are not receiving new messages from elsewhere...')
        time.sleep(constants.WATERMARK_STABILITY_CHECK_DELAY_SECS)

        watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
        second_check_watermarks_json = json.dumps(watermarks_by_topic)

        if first_check_watermarks_json != second_check_watermarks_json:
            raise Exception(f'Watermarks for one or more target topics changed between successive checks. '
                            f'Another process may be producing to the topic(s). Bailing.\nFirst check: '
                            f'{first_check_watermarks_json}\nSecond check: {second_check_watermarks_json}')
        logger.debug('Topic watermarks: %s', second_check_watermarks_json)

    prior_progress_log_table_data = []
    prior_progress = progress_tracker.get_prior_progress_or_create_progress_topic()

    for table in tables:
        snapshot_progress, changes_progress = None, None

        if not report_progress_only and table.topic_name not in watermarks_by_topic:  # new topic; create it
            if partition_count:
                this_topic_partition_count = partition_count
            else:
                per_second = table.get_change_rows_per_second()
                # one partition for each 10 rows/sec on average in the change table:
                this_topic_partition_count = max(1, int(per_second / 10))
                if this_topic_partition_count > 100:
                    raise Exception(
                        f'Automatic topic creation would create %{this_topic_partition_count} partitions for topic '
                        f'{table.topic_name} based on a change table rows per second rate of {per_second}. This '
                        f'seems excessive, so the program is exiting to prevent overwhelming your Kafka cluster. '
                        f'Look at setting PARTITION_COUNT to take manual control of this.')
            logger.info('Creating topic %s with %s partition(s)', table.topic_name, this_topic_partition_count)
            kafka_client.create_topic(table.topic_name, this_topic_partition_count, replication_factor,
                                      extra_topic_config)
        else:
            snapshot_progress: Union[None, progress_tracking.ProgressEntry] = prior_progress.get(
                (table.topic_name, constants.SNAPSHOT_ROWS_KIND))
            changes_progress: Union[None, progress_tracking.ProgressEntry] = prior_progress.get(
                (table.topic_name, constants.CHANGE_ROWS_KIND))

            fq_change_table_name = f'{constants.CDC_DB_SCHEMA_NAME}.{table.change_table_name}'
            if snapshot_progress and (snapshot_progress.change_table_name != fq_change_table_name):
                logger.info('Found prior snapshot progress into topic %s, but from an older capture instance '
                            '(prior progress instance: %s; current instance: %s)', table.topic_name,
                            snapshot_progress.change_table_name, fq_change_table_name)
                if redo_snapshot_for_new_instance:
                    if ddl_change_requires_new_snapshot(snapshot_progress.change_table_name, fq_change_table_name):
                        logger.info('Will start new snapshot.')
                    else:
                        logger.info('New snapshot does not appear to be required.')
                    snapshot_progress = None
                else:
                    logger.info('Will NOT start new snapshot.')

            if changes_progress and (changes_progress.change_table_name != fq_change_table_name):
                logger.info('Found prior change data progress into topic %s, but from an older capture instance '
                            '(prior progress instance: %s; current instance: %s)', table.topic_name,
                            changes_progress.change_table_name, fq_change_table_name)
                if publish_duplicate_changes_from_new_instance:
                    logger.info('Will republish any change rows duplicated by the new capture instance.')
                    changes_progress = None
                else:
                    logger.info('Will NOT republish any change rows duplicated by the new capture instance.')

        starting_change_index = (changes_progress and changes_progress.change_index) \
            or change_index.LOWEST_CHANGE_INDEX
        starting_snapshot_index = snapshot_progress and snapshot_progress.snapshot_index

        if report_progress_only:  # elide schema registration
            table.finalize_table(starting_change_index, starting_snapshot_index, options.LSN_GAP_HANDLING_IGNORE)
        else:
            table.finalize_table(starting_change_index, starting_snapshot_index, lsn_gap_handling,
                                 kafka_client.register_schemas, progress_tracker.reset_progress,
                                 progress_tracker.record_snapshot_completion)

        if not table.snapshot_allowed:
            snapshot_state = '<not doing>'
        elif table.snapshot_complete:
            snapshot_state = '<already complete>'
        elif table.last_read_key_for_snapshot_display is None:
            snapshot_state = '<from beginning>'
        else:
            snapshot_state = f'From {table.last_read_key_for_snapshot_display}'

        prior_progress_log_table_data.append((table.capture_instance_name, table.fq_name, table.topic_name,
                                              starting_change_index or '<from beginning>', snapshot_state))

    headers = ('Capture instance name', 'Source table name', 'Topic name', 'From change table index', 'Snapshots')
    table = tabulate(prior_progress_log_table_data, headers, tablefmt='fancy_grid')

    logger.info('Processing will proceed from the following positions based on the last message from each topic '
                'and/or the snapshot progress committed in Kafka (NB: snapshot reads occur BACKWARDS from high to '
                'low key column values):\n%s\n%s tables total.', table, len(prior_progress_log_table_data))


# noinspection PyUnusedLocal
def ddl_change_requires_new_snapshot(old_ci_name: str, new_ci_name: str) -> bool:
    return True  # TODO: this is a stub for a next-up planned feature
    

def should_terminate_due_to_capture_instance_change(
        db_conn: pyodbc.Connection, progress_tracker: progress_tracking.ProgressTracker,
        capture_instance_version_strategy: str, capture_instance_version_regex: str,
        capture_instance_to_topic_map: Dict[str, str], current_capture_instances: Dict[str, Dict[str, Any]],
        table_whitelist_regex: str, table_blacklist_regex: str,
        topic_to_max_polled_index_map: Dict[str, change_index.ChangeIndex]
) -> bool:
    new_capture_instances = get_latest_capture_instances_by_fq_name(
        db_conn, capture_instance_version_strategy, capture_instance_version_regex, table_whitelist_regex,
        table_blacklist_regex)

    current = {k: v['capture_instance_name'] for k, v in current_capture_instances.items()}
    new = {k: v['capture_instance_name'] for k, v in new_capture_instances.items()}

    if new == current:
        logger.debug('Capture instances unchanged; continuing...')
        return False

    def better_json_serialize(obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, (bytes,)):
            return f'0x{obj.hex()}'
        raise TypeError("Type %s not serializable" % type(obj))

    for fq_name, current_ci in current_capture_instances.items():
        if fq_name in new_capture_instances:
            new_ci = new_capture_instances[fq_name]
            if current_ci['capture_instance_name'] == new_ci['capture_instance_name']:
                continue
            topic = capture_instance_to_topic_map[current_ci['capture_instance_name']]
            current_idx = topic_to_max_polled_index_map.get(topic)
            if current_idx is not None:
                progress_tracker.emit_changes_progress_heartbeat(topic, current_idx)
            current_idx = current_idx or change_index.LOWEST_CHANGE_INDEX
            logger.info('Change detected in capture instance for %s.\nCurrent: %s\nNew: %s', fq_name,
                        json.dumps(current_ci, default=better_json_serialize),
                        json.dumps(new_ci, default=better_json_serialize))
            new_ci_min_index = change_index.ChangeIndex(new_ci['start_lsn'], b'\x00' * 10, 0)
            if current_idx < new_ci_min_index:
                with db_conn.cursor() as cursor:
                    ci_table_name = f"[{constants.CDC_DB_SCHEMA_NAME}].[{current_ci['capture_instance_name']}_CT]"
                    cursor.execute(f"SELECT TOP 1 1 FROM {ci_table_name} WITH (NOLOCK)")
                    has_rows = cursor.fetchval() is not None
                if has_rows:
                    logger.info('Progress against existing capture instance ("%s") for table "%s" has reached index '
                                '%s, but the new capture instance ("%s") does not begin until index %s. Deferring '
                                'termination to maintain data integrity and will try again on next capture instance '
                                'evaluation iteration.', current_ci['capture_instance_name'], fq_name, current_idx,
                                new_ci['capture_instance_name'], new_ci_min_index)
                    return False

    logger.warning('Terminating process due to change in capture instances. This behavior can be controlled by '
                   'changing option TERMINATE_ON_CAPTURE_INSTANCE_CHANGE.')
    return True
