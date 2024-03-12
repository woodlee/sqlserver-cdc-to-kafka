import argparse
import collections
import datetime
import functools
import heapq
import json
import logging
import re
import time
from typing import Dict, Optional, List, Tuple

import pyodbc

from . import clock_sync, kafka, tracked_tables, constants, options, validation, change_index, progress_tracking, \
    sql_query_subprocess, sql_queries, helpers, avro
from .build_startup_state import build_tracked_tables_from_cdc_metadata, determine_start_points_and_finalize_tables, \
    get_latest_capture_instances_by_fq_name, CaptureInstanceMetadata
from .metric_reporting import accumulator

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from . import parsed_row

logger = logging.getLogger(__name__)


def run() -> None:
    logger.info('Starting...')
    opts: argparse.Namespace
    opts, reporters = options.get_options_and_metrics_reporters()

    logger.debug('Parsed configuration: %s', json.dumps(vars(opts)))

    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.db_conn_string
            and opts.kafka_transactional_id):
        raise Exception('Arguments schema_registry_url, kafka_bootstrap_servers, db_conn_string, and transactional_id '
                        'are all required.')

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

        schema_generator: avro.AvroSchemaGenerator = avro.AvroSchemaGenerator(
            opts.always_use_avro_longs, opts.avro_type_spec_overrides)

        capture_instances_by_fq_name: Dict[str, CaptureInstanceMetadata] = get_latest_capture_instances_by_fq_name(
            db_conn, opts.capture_instance_version_strategy, opts.capture_instance_version_regex,
            opts.table_include_regex, opts.table_exclude_regex)

        if not capture_instances_by_fq_name:
            logger.error('No capture instances could be found.')
            exit(1)

        capture_instance_names: List[str] = [ci.capture_instance_name
                                             for ci in capture_instances_by_fq_name.values()]

        with kafka.KafkaClient(
            metrics_accumulator, opts.kafka_bootstrap_servers, opts.schema_registry_url,
            opts.extra_kafka_consumer_config, opts.extra_kafka_producer_config,
            disable_writing=opts.run_validations or opts.report_progress_only,
            transactional_id=opts.kafka_transactional_id
        ) as kafka_client:
            progress_tracker = progress_tracking.ProgressTracker(
                kafka_client, opts.progress_topic_name, opts.process_hostname, opts.snapshot_logging_topic_name
            )
            kafka_client.register_delivery_callback((
                constants.SINGLE_TABLE_CHANGE_MESSAGE, constants.UNIFIED_TOPIC_CHANGE_MESSAGE,
                constants.SINGLE_TABLE_SNAPSHOT_MESSAGE, constants.DELETION_CHANGE_TOMBSTONE_MESSAGE
            ), metrics_accumulator.kafka_delivery_callback)

            tables: List[tracked_tables.TrackedTable] = build_tracked_tables_from_cdc_metadata(
                db_conn, metrics_accumulator, opts.topic_name_template, opts.snapshot_table_include_regex,
                opts.snapshot_table_exclude_regex, opts.truncate_fields, capture_instance_names, opts.db_row_batch_size,
                sql_query_processor, schema_generator, progress_tracker)

            capture_instance_to_topic_map: Dict[str, str] = {
                t.capture_instance_name: t.topic_name for t in tables}

            determine_start_points_and_finalize_tables(
                kafka_client, db_conn, tables, schema_generator, progress_tracker, opts.lsn_gap_handling,
                opts.partition_count, opts.replication_factor, opts.extra_topic_config, opts.run_validations,
                redo_snapshot_for_new_instance, publish_duplicate_changes_from_new_instance, opts.report_progress_only)

            if opts.report_progress_only:
                exit(0)

            table_to_unified_topics_map: Dict[str, List[str]] = collections.defaultdict(list)
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

            last_metrics_emission_time: datetime.datetime = helpers.naive_utcnow()
            last_capture_instance_check_time: datetime.datetime = helpers.naive_utcnow()
            last_slow_table_heartbeat_time: datetime.datetime = helpers.naive_utcnow()
            next_cdc_poll_allowed_time: datetime.datetime = helpers.naive_utcnow()
            next_cdc_poll_due_time: datetime.datetime = helpers.naive_utcnow()
            last_produced_row: Optional['parsed_row.ParsedRow'] = None
            last_topic_produces: Dict[str, datetime.datetime] = {}
            change_rows_queue: List[Tuple[change_index.ChangeIndex, 'parsed_row.ParsedRow']] = []
            queued_change_row_counts: Dict[str, int] = {t.topic_name: 0 for t in tables}

            # Returned bool indicates whether the process should halt
            def poll_periodic_tasks() -> bool:
                nonlocal last_metrics_emission_time
                nonlocal last_slow_table_heartbeat_time
                nonlocal last_capture_instance_check_time

                kafka_client.begin_transaction()

                if (helpers.naive_utcnow() - last_metrics_emission_time) > constants.METRICS_REPORTING_INTERVAL:
                    start_time = time.perf_counter()
                    metrics = metrics_accumulator.end_and_get_values()
                    for reporter in reporters:
                        try:
                            reporter.emit(metrics)
                        except Exception as e:
                            logger.exception('Caught exception while reporting metrics', exc_info=e)
                    elapsed = (time.perf_counter() - start_time)
                    logger.debug('Metrics reporting completed in %s ms', elapsed * 1000)
                    metrics_accumulator.reset_and_start()
                    last_metrics_emission_time = helpers.naive_utcnow()

                if (helpers.naive_utcnow() - last_slow_table_heartbeat_time) > \
                        constants.SLOW_TABLE_PROGRESS_HEARTBEAT_INTERVAL:
                    for t in tables:
                        if not queued_change_row_counts[t.topic_name]:
                            last_topic_produce = last_topic_produces.get(t.topic_name)
                            if not last_topic_produce or (helpers.naive_utcnow() - last_topic_produce) > \
                                    2 * constants.SLOW_TABLE_PROGRESS_HEARTBEAT_INTERVAL:
                                logger.debug('Emitting heartbeat progress for slow table %s', t.fq_name)
                                progress_tracker.record_changes_progress(t.topic_name, t.max_polled_change_index)
                    last_slow_table_heartbeat_time = helpers.naive_utcnow()

                if opts.terminate_on_capture_instance_change and \
                        (helpers.naive_utcnow() - last_capture_instance_check_time) > \
                        constants.CHANGED_CAPTURE_INSTANCES_CHECK_INTERVAL:
                    topic_to_max_polled_index_map:  Dict[str, change_index.ChangeIndex] = {
                        t.topic_name: t.max_polled_change_index for t in tables
                    }
                    if should_terminate_due_to_capture_instance_change(
                            db_conn, progress_tracker, opts.capture_instance_version_strategy,
                            opts.capture_instance_version_regex, capture_instance_to_topic_map,
                            capture_instances_by_fq_name, opts.table_include_regex,
                            opts.table_exclude_regex, topic_to_max_polled_index_map):
                        kafka_client.commit_transaction()
                        return True
                    last_capture_instance_check_time = helpers.naive_utcnow()

                kafka_client.commit_transaction()
                return False

            logger.info('Beginning processing for %s tracked table(s).', len(tables))
            metrics_accumulator.reset_and_start()

            # The above is all setup, now we come to the "hot loop":

            try:
                row: 'parsed_row.ParsedRow'

                while True:
                    snapshots_remain: bool = not all([t.snapshot_complete for t in tables])
                    change_tables_lagging: bool = any([t.change_reads_are_lagging for t in tables])

                    # ----- Poll for and produce snapshot data while change row queries run -----

                    if snapshots_remain and not change_tables_lagging:
                        while helpers.naive_utcnow() < next_cdc_poll_due_time and snapshots_remain:
                            kafka_client.begin_transaction()
                            snapshot_progress_by_topic: Dict[str, Dict[str, str | int]] = {}
                            completions_to_log: List[functools.partial[None]] = []

                            for t in tables:
                                if not t.snapshot_complete:
                                    for row in t.retrieve_snapshot_query_results():
                                        kafka_client.produce(row.destination_topic, row.key_dict,
                                                             row.avro_key_schema_id, row.value_dict,
                                                             row.avro_value_schema_id,
                                                             constants.SINGLE_TABLE_SNAPSHOT_MESSAGE)
                                        snapshot_progress_by_topic[row.destination_topic] = row.key_dict
                                    if t.snapshot_complete:
                                        progress_tracker.record_snapshot_progress(
                                            t.topic_name, constants.SNAPSHOT_COMPLETION_SENTINEL)
                                        snapshot_progress_by_topic.pop(row.destination_topic, None)
                                        completions_to_log.append(functools.partial(
                                            progress_tracker.log_snapshot_completed, t.topic_name, t.fq_name,
                                            t.key_schema_id, t.value_schema_id, helpers.naive_utcnow(), row.key_dict
                                        ))
                                        snapshots_remain = not all([t.snapshot_complete for t in tables])
                                    else:
                                        t.enqueue_snapshot_query()   # NB: results may not be retrieved until next cycle

                                if helpers.naive_utcnow() > next_cdc_poll_due_time:
                                    break

                            for topic_name, snapshot_index in snapshot_progress_by_topic.items():
                                progress_tracker.record_snapshot_progress(topic_name, snapshot_index)

                            kafka_client.commit_transaction()

                            # This needs to happen outside the above Kafka transaction so that the topic
                            # watermarks captured as part of the logged event reflect the topic's state after
                            # the final snapshot row(s) have been produced:
                            if completions_to_log:
                                kafka_client.begin_transaction()
                                for completion in completions_to_log:
                                    completion()
                                kafka_client.commit_transaction()

                        if poll_periodic_tasks():
                            break

                    # ----- Wait for next poll window (if needed) and get ceiling LSN for cycle -----

                    if not change_tables_lagging:
                        wait_time = (next_cdc_poll_allowed_time - helpers.naive_utcnow()).total_seconds()
                        if wait_time > 0:
                            time.sleep(wait_time)
                            metrics_accumulator.register_sleep(wait_time)

                    if poll_periodic_tasks():
                        break

                    with db_conn.cursor() as cursor:
                        q, _ = sql_queries.get_max_lsn()
                        cursor.execute(q)
                        lsn_limit = cursor.fetchval()

                    next_cdc_poll_allowed_time = (helpers.naive_utcnow() + constants.MIN_CDC_POLLING_INTERVAL)
                    next_cdc_poll_due_time = (helpers.naive_utcnow() + constants.MAX_CDC_POLLING_INTERVAL)

                    # ----- Query for change rows ----

                    for t in tables:
                        if queued_change_row_counts[t.topic_name] < opts.db_row_batch_size + 1:
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

                    kafka_client.begin_transaction()
                    progress_by_topic: Dict[str, change_index.ChangeIndex] = {}

                    while change_rows_queue:
                        row = heapq.heappop(change_rows_queue)[1]

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
                        last_topic_produces[row.destination_topic] = helpers.naive_utcnow()

                        if not opts.disable_deletion_tombstones and row.operation_name == \
                                constants.DELETE_OPERATION_NAME:
                            kafka_client.produce(row.destination_topic, row.key_dict, row.avro_key_schema_id,
                                                 None, row.avro_value_schema_id,
                                                 constants.DELETION_CHANGE_TOMBSTONE_MESSAGE)

                        progress_by_topic[row.destination_topic] = row.change_idx

                    for topic_name, progress_index in progress_by_topic.items():
                        progress_tracker.record_changes_progress(topic_name, progress_index)
                    kafka_client.commit_transaction()

                    if poll_periodic_tasks():
                        break
            except (KeyboardInterrupt, pyodbc.OperationalError):
                logger.info('Exiting due to external interrupt.')


def should_terminate_due_to_capture_instance_change(
        db_conn: pyodbc.Connection, progress_tracker: progress_tracking.ProgressTracker,
        capture_instance_version_strategy: str, capture_instance_version_regex: str,
        capture_instance_to_topic_map: Dict[str, str], current_capture_instances: Dict[str, CaptureInstanceMetadata],
        table_include_regex: str, table_exclude_regex: str,
        topic_to_max_polled_index_map: Dict[str, change_index.ChangeIndex]
) -> bool:
    new_capture_instances: Dict[str, CaptureInstanceMetadata] = get_latest_capture_instances_by_fq_name(
        db_conn, capture_instance_version_strategy, capture_instance_version_regex, table_include_regex,
        table_exclude_regex)

    current = {k: (v.capture_instance_name, v.types_checksum) for k, v in current_capture_instances.items()}
    new = {k: (v.capture_instance_name, v.types_checksum) for k, v in new_capture_instances.items()}

    if new == current:
        logger.debug('Capture instances unchanged; continuing...')
        return False

    for fq_name, current_ci in current_capture_instances.items():
        if fq_name in new_capture_instances:
            new_ci = new_capture_instances[fq_name]
            if (current_ci.capture_instance_name == new_ci.capture_instance_name and
                    current_ci.types_checksum == new_ci.types_checksum):
                continue
            topic = capture_instance_to_topic_map[current_ci.capture_instance_name]
            if topic in topic_to_max_polled_index_map:
                progress_tracker.record_changes_progress(topic, topic_to_max_polled_index_map[topic])
            last_recorded_progress = progress_tracker.get_last_recorded_progress_for_topic(topic)
            current_idx = last_recorded_progress and last_recorded_progress.change_index or \
                change_index.LOWEST_CHANGE_INDEX
            logger.info('Change detected in capture instance for %s.\nCurrent: capture instance name "%s", column '
                        'types checksum %s\nNew: capture instance name "%s", column types checksum %s',
                        fq_name, current_ci.capture_instance_name, current_ci.types_checksum,
                        new_ci.capture_instance_name, new_ci.types_checksum)
            new_ci_min_index = change_index.ChangeIndex(new_ci.start_lsn, b'\x00' * 10, 0)
            if current_idx < new_ci_min_index:
                with db_conn.cursor() as cursor:
                    change_table_name = helpers.quote_name(
                        helpers.get_fq_change_table_name(current_ci.capture_instance_name))
                    cursor.execute(f"SELECT TOP 1 1 FROM {change_table_name} WITH (NOLOCK)")
                    has_rows = cursor.fetchval() is not None
                if has_rows:
                    logger.info('Progress against existing capture instance ("%s") for table "%s" has reached index '
                                '%s, but the new capture instance ("%s") does not begin until index %s. Deferring '
                                'termination to maintain data integrity and will try again on next capture instance '
                                'evaluation iteration.', current_ci.capture_instance_name, fq_name, current_idx,
                                new_ci.capture_instance_name, new_ci_min_index)
                    return False

    logger.warning('Terminating process due to change in capture instances. This behavior can be controlled by '
                   'changing option TERMINATE_ON_CAPTURE_INSTANCE_CHANGE.')
    return True
