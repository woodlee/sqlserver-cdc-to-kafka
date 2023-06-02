import collections
import json
import logging
import re
import time
from typing import Dict, List, Tuple, Iterable, Union, Optional, Any, Set

import pyodbc
from tabulate import tabulate

from . import sql_query_subprocess, tracked_tables, sql_queries, kafka, progress_tracking, change_index, \
    constants, helpers, options, avro_from_sql
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


def build_tracked_tables_from_cdc_metadata(
    db_conn: pyodbc.Connection, metrics_accumulator: 'accumulator.Accumulator', topic_name_template: str,
    snapshot_table_whitelist_regex: str, snapshot_table_blacklist_regex: str, truncate_fields: Dict[str, int],
    capture_instance_names: List[str], db_row_batch_size: int, force_avro_long: bool,
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
            db_conn,  metrics_accumulator, sql_query_processor, schema_name, table_name, capture_instance_name,
            topic_name, min_lsn, can_snapshot, db_row_batch_size)

        for (change_table_ordinal, column_name, sql_type_name, is_computed, primary_key_ordinal, decimal_precision,
             decimal_scale, is_nullable) in fields:
            truncate_after = truncate_fields.get(f'{schema_name}.{table_name}.{column_name}'.lower())
            tracked_table.append_field(tracked_tables.TrackedField(
                column_name, sql_type_name, change_table_ordinal, primary_key_ordinal, decimal_precision,
                decimal_scale, force_avro_long, truncate_after))

        result.append(tracked_table)

    return result


def determine_start_points_and_finalize_tables(
        kafka_client: kafka.KafkaClient, db_conn: pyodbc.Connection, tables: Iterable[tracked_tables.TrackedTable],
        progress_tracker: progress_tracking.ProgressTracker, lsn_gap_handling: str,
        partition_count: int, replication_factor: int, extra_topic_config: Dict[str, Union[str, int]],
        force_avro_long: bool, validation_mode: bool = False, redo_snapshot_for_new_instance: bool = False,
        publish_duplicate_changes_from_new_instance: bool = False, report_progress_only: bool = False
) -> None:
    topic_names: List[str] = [t.topic_name for t in tables]

    if validation_mode:
        for table in tables:
            table.snapshot_allowed = False
            table.finalize_table(change_index.LOWEST_CHANGE_INDEX, None, {}, lsn_gap_handling)
        return

    if report_progress_only:
        watermarks_by_topic = []
    else:
        watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
        first_check_watermarks_json = json.dumps(watermarks_by_topic)

        logger.info(f'Pausing for {constants.WATERMARK_STABILITY_CHECK_DELAY_SECS} seconds to ensure target topics are '
                    f'not receiving new messages from elsewhere...')
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
        prior_change_table_max_index: Optional[change_index.ChangeIndex] = None

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

            fq_change_table_name = helpers.get_fq_change_table_name(table.capture_instance_name)
            if snapshot_progress and (snapshot_progress.change_table_name != fq_change_table_name):
                logger.info('Found prior snapshot progress into topic %s, but from an older capture instance '
                            '(prior progress instance: %s; current instance: %s)', table.topic_name,
                            snapshot_progress.change_table_name, fq_change_table_name)
                if redo_snapshot_for_new_instance:
                    old_capture_instance_name = helpers.get_capture_instance_name(snapshot_progress.change_table_name)
                    new_capture_instance_name = helpers.get_capture_instance_name(fq_change_table_name)
                    if ddl_change_requires_new_snapshot(db_conn, old_capture_instance_name, new_capture_instance_name,
                                                        table.fq_name, force_avro_long):
                        logger.info('Will start new snapshot.')
                        snapshot_progress = None
                    else:
                        progress_tracker.record_snapshot_completion(table.topic_name)
                else:
                    progress_tracker.record_snapshot_completion(table.topic_name)
                    logger.info('Will NOT start new snapshot.')

            if changes_progress and (changes_progress.change_table_name != fq_change_table_name):
                logger.info('Found prior change data progress into topic %s, but from an older capture instance '
                            '(prior progress instance: %s; current instance: %s)', table.topic_name,
                            changes_progress.change_table_name, fq_change_table_name)
                with db_conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(?)",
                                   changes_progress.change_table_name)
                    if cursor.fetchval() is not None:
                        q, p = sql_queries.get_max_lsn_for_change_table(
                            helpers.quote_name(changes_progress.change_table_name))
                        cursor.execute(q)
                        res = cursor.fetchone()
                        if res:
                            (lsn, _, seqval, operation) = res
                            prior_change_table_max_index = change_index.ChangeIndex(lsn, seqval, operation)

                if publish_duplicate_changes_from_new_instance:
                    logger.info('Will republish any change rows duplicated by the new capture instance.')
                    changes_progress = None
                else:
                    logger.info('Will NOT republish any change rows duplicated by the new capture instance.')

        starting_change_index = (changes_progress and changes_progress.change_index) \
            or change_index.LOWEST_CHANGE_INDEX
        starting_snapshot_index = snapshot_progress and snapshot_progress.snapshot_index

        if report_progress_only:  # elide schema registration
            table.finalize_table(starting_change_index, prior_change_table_max_index, starting_snapshot_index,
                                 options.LSN_GAP_HANDLING_IGNORE)
        else:
            table.finalize_table(starting_change_index, prior_change_table_max_index, starting_snapshot_index,
                                 lsn_gap_handling, kafka_client.register_schemas, progress_tracker.reset_progress,
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
    table = tabulate(sorted(prior_progress_log_table_data), headers, tablefmt='fancy_grid')

    logger.info('Processing will proceed from the following positions based on the last message from each topic '
                'and/or the snapshot progress committed in Kafka (NB: snapshot reads occur BACKWARDS from high to '
                'low key column values):\n%s\n%s tables total.', table, len(prior_progress_log_table_data))


def ddl_change_requires_new_snapshot(db_conn: pyodbc.Connection, old_capture_instance_name: str,
                                     new_capture_instance_name: str, source_table_fq_name: str, force_avro_long: bool,
                                     resnapshot_for_column_drops: bool = True) -> bool:
    with db_conn.cursor() as cursor:
        cursor.execute(f'SELECT TOP 1 1 FROM [{constants.CDC_DB_SCHEMA_NAME}].[change_tables] '
                       f'WHERE capture_instance = ?', old_capture_instance_name)
        if not cursor.fetchval():
            logger.info('Requiring re-snapshot for %s because prior capture instance %s is no longer available as a '
                        'basis for evaluating schema changes.', source_table_fq_name, old_capture_instance_name)
            return True

        q, p = sql_queries.get_cdc_tracked_tables_metadata([old_capture_instance_name, new_capture_instance_name])
        cursor.execute(q)
        old_cols: Dict[str, Dict[str, Any]] = {}
        new_cols: Dict[str, Dict[str, Any]] = {}
        for row in cursor.fetchall():
            (_, _, capture_instance_name, _, _, column_name, sql_type_name, is_computed, _, decimal_precision,
             decimal_scale, is_nullable) = row
            col_info = {'sql_type_name': sql_type_name,
                        'decimal_precision': decimal_precision,
                        'decimal_scale': decimal_scale,
                        'is_computed': is_computed,
                        'is_nullable': is_nullable}
            if capture_instance_name == old_capture_instance_name:
                old_cols[column_name] = col_info
            elif capture_instance_name == new_capture_instance_name:
                new_cols[column_name] = col_info

        added_col_names = new_cols.keys() - old_cols.keys()
        removed_col_names = old_cols.keys() - new_cols.keys()
        changed_col_names = {k for k in new_cols.keys()
                             if k in old_cols
                             and old_cols[k] != new_cols[k]}
        logger.info('Evaluating need for new snapshot in change from capture instance %s to %s. Added cols: %s Removed '
                    'cols: %s Cols with type changes: %s ...', old_capture_instance_name, new_capture_instance_name,
                    added_col_names, removed_col_names, changed_col_names)

        if removed_col_names and resnapshot_for_column_drops:
            logger.info('Requiring re-snapshot for %s because the new capture instance removes column(s) %s.',
                        source_table_fq_name, removed_col_names)
            return True

        for changed_col_name in changed_col_names:
            old_col = old_cols[changed_col_name]
            new_col = new_cols[changed_col_name]
            # Even if the DB col type changed, a resnapshot is really only needed if the corresponding Avro type
            # changes. An example would be a column "upgrading" from SMALLINT to INT:
            old_avro_type = avro_from_sql.avro_schema_from_sql_type(changed_col_name, old_col['sql_type_name'],
                                                                    old_col['decimal_precision'],
                                                                    old_col['decimal_scale'], True, force_avro_long)
            new_avro_type = avro_from_sql.avro_schema_from_sql_type(changed_col_name, new_col['sql_type_name'],
                                                                    new_col['decimal_precision'],
                                                                    new_col['decimal_scale'], True, force_avro_long)
            if old_col['is_computed'] != new_col['is_computed'] or old_avro_type != new_avro_type:
                logger.info('Requiring re-snapshot for %s due to a data type change for column %s (type: %s, '
                            'is_computed: %s --> type: %s, is_computed: %s).', source_table_fq_name, changed_col_name,
                            old_col['sql_type_name'], old_col['is_computed'], new_col['sql_type_name'],
                            new_col['is_computed'])
                return True

        for added_col_name in added_col_names:
            col_info = new_cols[added_col_name]
            if not col_info['is_nullable']:
                logger.info('Requiring re-snapshot for %s because newly-captured column %s is marked NOT NULL',
                            source_table_fq_name, added_col_name)
                return True

        quoted_fq_name = helpers.quote_name(source_table_fq_name)
        q, p = sql_queries.get_table_rowcount_bounded(quoted_fq_name, constants.SMALL_TABLE_THRESHOLD)
        cursor.execute(q)
        bounded_row_count = cursor.fetchval()
        logger.debug('Bounded row count for %s was: %s', source_table_fq_name, bounded_row_count)
        table_is_small = bounded_row_count < constants.SMALL_TABLE_THRESHOLD

        # Gets the names of columns that appear in the first position of one or more unfiltered, non-disabled indexes:
        q, p = sql_queries.get_indexed_cols()
        cursor.setinputsizes(p)
        cursor.execute(q, source_table_fq_name)
        indexed_cols: Set[str] = {row[0] for row in cursor.fetchall()}
        recently_added_cols: Optional[Set[str]] = None

        for added_col_name in added_col_names:
            if table_is_small or added_col_name in indexed_cols:
                cursor.execute(f"SELECT TOP 1 1 FROM {quoted_fq_name} WITH (NOLOCK) "
                               f"WHERE [{added_col_name}] IS NOT NULL")
                if cursor.fetchval() is not None:
                    logger.info('Requiring re-snapshot for %s because a direct scan of newly-tracked column %s '
                                'detected non-null values.', source_table_fq_name, added_col_name)
                    return True
                else:
                    logger.info('New col %s on table %s contains only NULL values per direct check.',
                                added_col_name, source_table_fq_name)
            else:
                # if we get here it means the table is large, the new column does not lead in an index, but
                # the new column is nullable.
                if recently_added_cols is None:
                    cols_with_too_old_changes: Set[str] = set()
                    cols_with_new_enough_changes: Set[str] = set()
                    q, p = sql_queries.get_ddl_history_for_capture_table()
                    cursor.setinputsizes(p)
                    cursor.execute(q, helpers.get_fq_change_table_name(old_capture_instance_name))
                    alter_re = re.compile(
                        r'\W*alter\s+table\s+(?P<table>[\w\.\[\]]+)\s+add\s+(?P<column>[\w\.\[\]]+)\s+(?P<spec>.*)',
                        re.IGNORECASE)
                    for (ddl_command, age_seconds) in cursor.fetchall():
                        match = alter_re.match(ddl_command)
                        if match and match.groupdict().get('column'):
                            col_name_lower = match.groupdict()['column'].lower()
                            if age_seconds > constants.MAX_AGE_TO_PRESUME_ADDED_COL_IS_NULL_SECONDS:
                                cols_with_too_old_changes.add(col_name_lower)
                            else:
                                cols_with_new_enough_changes.add(col_name_lower)
                    recently_added_cols = cols_with_new_enough_changes - cols_with_too_old_changes

                if added_col_name.lower() not in recently_added_cols:
                    logger.info('Requiring re-snapshot for %s because newly-tracked column %s appears to have been '
                                'added more than %s seconds ago.', source_table_fq_name, added_col_name,
                                constants.MAX_AGE_TO_PRESUME_ADDED_COL_IS_NULL_SECONDS)
                    return True
                else:
                    logger.info('New col %s on table %s is ASSUMED to contain only NULL values because of the recency '
                                'of its addition.', added_col_name, source_table_fq_name)

    logger.info('Not requiring re-snapshot for table %s.', source_table_fq_name)
    return False


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
