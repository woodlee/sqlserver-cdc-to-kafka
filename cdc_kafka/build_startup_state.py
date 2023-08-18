import collections
import datetime
import logging
import re
from typing import Dict, List, Tuple, Iterable, Optional, Any, Set, NamedTuple, Mapping

import pyodbc
from tabulate import tabulate

from . import sql_query_subprocess, tracked_tables, sql_queries, kafka, progress_tracking, change_index, \
    constants, helpers, options, avro
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


def build_tracked_tables_from_cdc_metadata(
    db_conn: pyodbc.Connection, metrics_accumulator: accumulator.Accumulator, topic_name_template: str,
    snapshot_table_include_config: str, snapshot_table_exclude_config: str, truncate_fields: Dict[str, int],
    capture_instance_names: List[str], db_row_batch_size: int,
    sql_query_processor: sql_query_subprocess.SQLQueryProcessor,
    schema_generator: avro.AvroSchemaGenerator
) -> List[tracked_tables.TrackedTable]:
    result: List[tracked_tables.TrackedTable] = []

    truncate_fields = {k.lower(): v for k, v in truncate_fields.items()}

    snapshot_table_include_regex = snapshot_table_include_config and re.compile(
        snapshot_table_include_config, re.IGNORECASE)
    snapshot_table_exclude_regex = snapshot_table_exclude_config and re.compile(
        snapshot_table_exclude_config, re.IGNORECASE)

    name_to_meta_fields: Dict[Tuple[Any, ...], List[Tuple[Any, ...]]] \
        = collections.defaultdict(list)

    with db_conn.cursor() as cursor:
        q, _ = sql_queries.get_cdc_tracked_tables_metadata(capture_instance_names)
        cursor.execute(q)
        for row in cursor.fetchall():
            # 0:4 gets schema name, table name, capture instance name, min captured LSN:
            name_to_meta_fields[tuple(row[0:4])].append(row[4:])

    for (schema_name, table_name, capture_instance_name, min_lsn), fields in name_to_meta_fields.items():
        fq_table_name = f'{schema_name}.{table_name}'

        can_snapshot = False

        if snapshot_table_include_regex and snapshot_table_include_regex.match(fq_table_name):
            logger.debug('Table %s matched snapshotting inclusion regex', fq_table_name)
            can_snapshot = True

        if snapshot_table_exclude_regex and snapshot_table_exclude_regex.match(fq_table_name):
            logger.debug('Table %s matched snapshotting exclusion regex and will NOT be snapshotted', fq_table_name)
            can_snapshot = False

        topic_name = topic_name_template.format(
            schema_name=schema_name, table_name=table_name, capture_instance_name=capture_instance_name)

        tracked_table = tracked_tables.TrackedTable(
            db_conn,  metrics_accumulator, sql_query_processor, schema_generator, schema_name, table_name,
            capture_instance_name, topic_name, min_lsn, can_snapshot, db_row_batch_size)

        for (change_table_ordinal, column_name, sql_type_name, _, primary_key_ordinal, decimal_precision,
             decimal_scale, _) in fields:
            truncate_after: int = truncate_fields.get(f'{schema_name}.{table_name}.{column_name}'.lower(), 0)
            tracked_table.append_field(tracked_tables.TrackedField(
                column_name, sql_type_name, change_table_ordinal, primary_key_ordinal, decimal_precision,
                decimal_scale, truncate_after))

        result.append(tracked_table)

    return result


def determine_start_points_and_finalize_tables(
        kafka_client: kafka.KafkaClient, db_conn: pyodbc.Connection, tables: Iterable[tracked_tables.TrackedTable],
        schema_generator: avro.AvroSchemaGenerator, progress_tracker: progress_tracking.ProgressTracker,
        lsn_gap_handling: str, partition_count: int, replication_factor: int,
        extra_topic_config: Dict[str, str | int], validation_mode: bool = False,
        redo_snapshot_for_new_instance: bool = False, publish_duplicate_changes_from_new_instance: bool = False,
        report_progress_only: bool = False
) -> None:
    if validation_mode:
        for table in tables:
            table.snapshot_allowed = False
            table.finalize_table(change_index.LOWEST_CHANGE_INDEX, None, {}, lsn_gap_handling)
        return

    prior_progress_log_table_data = []
    prior_progress = progress_tracker.get_prior_progress_or_create_progress_topic()

    snapshot_progress: Optional[progress_tracking.ProgressEntry]
    changes_progress: Optional[progress_tracking.ProgressEntry]

    for table in tables:
        kafka_client.begin_transaction()
        snapshot_progress, changes_progress = None, None
        prior_change_table_max_index: Optional[change_index.ChangeIndex] = None

        if not report_progress_only and kafka_client.get_topic_partition_count(
                table.topic_name) == 0:  # new topic; create it
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
            snapshot_progress = prior_progress.get((table.topic_name, constants.SNAPSHOT_ROWS_KIND))
            changes_progress = prior_progress.get((table.topic_name, constants.CHANGE_ROWS_KIND))

            fq_change_table_name = helpers.get_fq_change_table_name(table.capture_instance_name)
            if snapshot_progress and (snapshot_progress.change_table_name != fq_change_table_name):
                logger.info('Found prior snapshot progress into topic %s, but from an older capture instance '
                            '(prior progress instance: %s; current instance: %s)', table.topic_name,
                            snapshot_progress.change_table_name, fq_change_table_name)
                if redo_snapshot_for_new_instance:
                    old_capture_instance_name = helpers.get_capture_instance_name(snapshot_progress.change_table_name)
                    new_capture_instance_name = helpers.get_capture_instance_name(fq_change_table_name)
                    if ddl_change_requires_new_snapshot(db_conn, schema_generator, old_capture_instance_name,
                                                        new_capture_instance_name, table.fq_name):
                        logger.info('Will start new snapshot.')
                        snapshot_progress = None
                    else:
                        progress_tracker.record_snapshot_progress(table.topic_name,
                                                                  constants.SNAPSHOT_COMPLETION_SENTINEL)
                else:
                    progress_tracker.record_snapshot_progress(table.topic_name,
                                                              constants.SNAPSHOT_COMPLETION_SENTINEL)
                    logger.info('Will NOT start new snapshot.')

            if changes_progress and (changes_progress.change_table_name != fq_change_table_name):
                logger.info('Found prior change data progress into topic %s, but from an older capture instance '
                            '(prior progress instance: %s; current instance: %s)', table.topic_name,
                            changes_progress.change_table_name, fq_change_table_name)
                with db_conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(?)",
                                   changes_progress.change_table_name)
                    if cursor.fetchval() is not None:
                        q, _ = sql_queries.get_max_lsn_for_change_table(
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

        starting_change_index: change_index.ChangeIndex = \
            (changes_progress and changes_progress.change_index) or change_index.LOWEST_CHANGE_INDEX
        starting_snapshot_index: Optional[Mapping[str, str | int]] = None
        if snapshot_progress:
            starting_snapshot_index = snapshot_progress.snapshot_index

        if report_progress_only:  # elide schema registration
            table.finalize_table(starting_change_index, prior_change_table_max_index, starting_snapshot_index,
                                 options.LSN_GAP_HANDLING_IGNORE)
        else:
            table.finalize_table(starting_change_index, prior_change_table_max_index, starting_snapshot_index,
                                 lsn_gap_handling, kafka_client.register_schemas, progress_tracker.reset_progress,
                                 progress_tracker.record_snapshot_progress)

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
        kafka_client.commit_transaction()

    headers = ('Capture instance name', 'Source table name', 'Topic name', 'From change table index', 'Snapshots')
    display_table = tabulate(sorted(prior_progress_log_table_data), headers, tablefmt='fancy_grid')

    logger.info('Processing will proceed from the following positions based on the last message from each topic '
                'and/or the snapshot progress committed in Kafka (NB: snapshot reads occur BACKWARDS from high to '
                'low key column values):\n%s\n%s tables total.', display_table, len(prior_progress_log_table_data))


def ddl_change_requires_new_snapshot(db_conn: pyodbc.Connection, schema_generator: avro.AvroSchemaGenerator,
                                     old_capture_instance_name: str, new_capture_instance_name: str,
                                     source_table_fq_name: str, resnapshot_for_column_drops: bool = True) -> bool:
    with db_conn.cursor() as cursor:
        cursor.execute(f'SELECT TOP 1 1 FROM [{constants.CDC_DB_SCHEMA_NAME}].[change_tables] '
                       f'WHERE capture_instance = ?', old_capture_instance_name)
        if not cursor.fetchval():
            logger.info('Requiring re-snapshot for %s because prior capture instance %s is no longer available as a '
                        'basis for evaluating schema changes.', source_table_fq_name, old_capture_instance_name)
            return True

        q, _ = sql_queries.get_cdc_tracked_tables_metadata([old_capture_instance_name, new_capture_instance_name])
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
            db_schema, db_table = source_table_fq_name.split('.')
            old_avro_type = schema_generator.get_record_field_schema(
                db_schema, db_table, changed_col_name, old_col['sql_type_name'], old_col['decimal_precision'],
                old_col['decimal_scale'], True)
            new_avro_type = schema_generator.get_record_field_schema(
                db_schema, db_table, changed_col_name, new_col['sql_type_name'], new_col['decimal_precision'],
                new_col['decimal_scale'], True)
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
        q, _ = sql_queries.get_table_rowcount_bounded(quoted_fq_name, constants.SMALL_TABLE_THRESHOLD)
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


class CaptureInstanceMetadata(NamedTuple):
    fq_name: str
    capture_instance_name: str
    start_lsn: bytes
    create_date: datetime.datetime
    types_checksum: int
    regex_matched_group: str


# This pulls the "greatest" capture instance running for each source table, in the event there is more than one.
def get_latest_capture_instances_by_fq_name(
        db_conn: pyodbc.Connection, capture_instance_version_strategy: str, capture_instance_version_config: str,
        table_include_config: str, table_exclude_config: str
) -> Dict[str, CaptureInstanceMetadata]:
    if capture_instance_version_strategy == options.CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX \
            and not capture_instance_version_config:
        raise Exception('Please provide a capture_instance_version_regex when specifying the `regex` '
                        'capture_instance_version_strategy.')
    result: Dict[str, CaptureInstanceMetadata] = {}
    fq_name_to_capture_instances: Dict[str, List[CaptureInstanceMetadata]] = collections.defaultdict(list)
    capture_instance_version_regex = capture_instance_version_config and re.compile(capture_instance_version_config)
    table_include_regex = table_include_config and re.compile(table_include_config, re.IGNORECASE)
    table_exclude_regex = table_exclude_config and re.compile(table_exclude_config, re.IGNORECASE)

    with db_conn.cursor() as cursor:
        q, _ = sql_queries.get_cdc_capture_instances_metadata()
        cursor.execute(q)
        for row in cursor.fetchall():
            fq_table_name = f'{row[0]}.{row[1]}'

            if table_include_regex and not table_include_regex.match(fq_table_name):
                logger.debug('Table %s excluded; did not match inclusion regex', fq_table_name)
                continue

            if table_exclude_regex and table_exclude_regex.match(fq_table_name):
                logger.debug('Table %s excluded; matched exclusion regex', fq_table_name)
                continue

            if row[3] is None or row[4] is None:
                logger.debug('Capture instance for %s appears to be brand-new; will evaluate again on '
                             'next pass', fq_table_name)
                continue

            regex_matched_group: str = ''
            if capture_instance_version_regex:
                match = capture_instance_version_regex.match(row[1])
                regex_matched_group = match and match.group(1) or ''

            ci_meta = CaptureInstanceMetadata(fq_table_name, row[2], row[3], row[4], row[5], regex_matched_group)
            fq_name_to_capture_instances[ci_meta.fq_name].append(ci_meta)

    for fq_name, capture_instances in fq_name_to_capture_instances.items():
        if capture_instance_version_strategy == options.CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE:
            latest_instance = sorted(capture_instances, key=lambda x: x.create_date)[-1]
        elif capture_instance_version_strategy == options.CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX:
            latest_instance = sorted(capture_instances, key=lambda x: x.regex_matched_group)[-1]
        else:
            raise Exception(f'Capture instance version strategy "{capture_instance_version_strategy}" not recognized.')
        result[fq_name] = latest_instance

    logger.debug('Latest capture instance names determined by "%s" strategy: %s', capture_instance_version_strategy,
                 sorted([v.capture_instance_name for v in result.values()]))

    return result
