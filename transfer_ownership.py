#!/usr/bin/env python3

import argparse
import collections
import json
import logging
import os
import socket
from typing import Dict, List, Optional, Tuple, Any, NamedTuple

import confluent_kafka.avro
import pyodbc
from tabulate import tabulate

from cdc_kafka import constants, helpers, kafka, options, progress_tracking, sql_queries
from cdc_kafka.build_startup_state import CaptureInstanceMetadata, get_latest_capture_instances_by_fq_name
from cdc_kafka.change_index import ChangeIndex
from cdc_kafka.metric_reporting import accumulator
from cdc_kafka.serializers.avro import AvroSchemaGenerator, AvroSerializer, AVRO_SCHEMA_NAMESPACE

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)


class ColumnMetadata(NamedTuple):
    column_name: str
    sql_type_name: str
    change_table_ordinal: int
    primary_key_ordinal: Optional[int]
    decimal_precision: int
    decimal_scale: int
    is_nullable: bool


class TableTransferPlan(NamedTuple):
    fq_name: str
    topic_name: str
    source_capture_instance: str
    target_capture_instance: str
    source_key_fields: List[str]
    target_key_fields: List[str]
    snapshot_action: str
    change_action: str
    prior_change_progress: Optional[ChangeIndex]
    new_change_index: Optional[ChangeIndex]
    prior_snapshot_progress: Optional[Any]
    new_snapshot_progress: Optional[Any]
    warnings: List[str]
    schema_compatible: Optional[bool]


def get_capture_instances_with_columns(
        db_conn: pyodbc.Connection, capture_instance_version_strategy: str,
        capture_instance_version_config: str, table_include_config: str, table_exclude_config: str
) -> Tuple[Dict[str, CaptureInstanceMetadata], Dict[str, List[ColumnMetadata]]]:
    instances = get_latest_capture_instances_by_fq_name(
        db_conn, capture_instance_version_strategy, capture_instance_version_config,
        table_include_config, table_exclude_config)

    columns_by_fq_name: Dict[str, List[ColumnMetadata]] = collections.defaultdict(list)

    if not instances:
        return instances, columns_by_fq_name

    capture_instance_names = [ci.capture_instance_name for ci in instances.values()]
    with db_conn.cursor() as cursor:
        q, _ = sql_queries.get_cdc_tracked_tables_metadata(capture_instance_names)
        cursor.execute(q)
        for row in cursor.fetchall():
            (schema_name, table_name, capture_instance_name, _, change_table_ordinal,
             column_name, sql_type_name, _, primary_key_ordinal, decimal_precision,
             decimal_scale, is_nullable) = row
            fq_name = f'{schema_name}.{table_name}'
            columns_by_fq_name[fq_name].append(ColumnMetadata(
                column_name, sql_type_name, change_table_ordinal, primary_key_ordinal,
                decimal_precision, decimal_scale, is_nullable))

    return instances, columns_by_fq_name


def get_key_fields_for_table(columns: List[ColumnMetadata]) -> List[str]:
    key_cols = [(c.column_name, c.primary_key_ordinal) for c in columns if c.primary_key_ordinal is not None]
    if not key_cols:
        return [constants.MESSAGE_KEY_FIELD_NAME_WHEN_PK_ABSENT]
    return [name for name, _ in sorted(key_cols, key=lambda x: x[1])]


def get_max_change_index_for_capture_instance(db_conn: pyodbc.Connection, capture_instance_name: str) -> Optional[ChangeIndex]:
    fq_change_table_name = helpers.get_fq_change_table_name(capture_instance_name)
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(?)", fq_change_table_name)
        if cursor.fetchval() is None:
            return None
        q, _ = sql_queries.get_max_lsn_for_change_table(helpers.quote_name(fq_change_table_name))
        cursor.execute(q)
        res = cursor.fetchone()
        if res:
            (lsn, command_id, seqval, operation) = res
            return ChangeIndex(lsn, command_id, seqval, operation)
    return None


def check_schema_compatibility(
        schema_registry_url: str, topic_name: str,
        source_columns: List[ColumnMetadata], target_columns: List[ColumnMetadata],
        schema_generator: AvroSchemaGenerator, fq_name: str
) -> Tuple[bool, List[str]]:
    warnings: List[str] = []
    schema_name, table_name = fq_name.split('.')

    source_key_cols = sorted([c for c in source_columns if c.primary_key_ordinal is not None],
                             key=lambda c: c.primary_key_ordinal)
    target_key_cols = sorted([c for c in target_columns if c.primary_key_ordinal is not None],
                             key=lambda c: c.primary_key_ordinal)

    source_key_fields = []
    for c in source_key_cols:
        source_key_fields.append(schema_generator.get_record_field_schema(
            schema_name, table_name, c.column_name, c.sql_type_name,
            c.decimal_precision, c.decimal_scale, False))

    target_key_fields = []
    for c in target_key_cols:
        target_key_fields.append(schema_generator.get_record_field_schema(
            schema_name, table_name, c.column_name, c.sql_type_name,
            c.decimal_precision, c.decimal_scale, False))

    if source_key_fields != target_key_fields:
        warnings.append(f'Key schema would change for topic {topic_name}')
        return False, warnings

    source_value_cols = sorted(source_columns, key=lambda c: c.change_table_ordinal)
    target_value_cols = sorted(target_columns, key=lambda c: c.change_table_ordinal)

    source_value_field_names = [c.column_name for c in source_value_cols]
    target_value_field_names = [c.column_name for c in target_value_cols]

    source_value_fields = []
    for c in source_value_cols:
        source_value_fields.append(schema_generator.get_record_field_schema(
            schema_name, table_name, c.column_name, c.sql_type_name,
            c.decimal_precision, c.decimal_scale, True))

    target_value_fields = []
    for c in target_value_cols:
        target_value_fields.append(schema_generator.get_record_field_schema(
            schema_name, table_name, c.column_name, c.sql_type_name,
            c.decimal_precision, c.decimal_scale, True))

    source_value_schema_json = {
        "name": f"{schema_name}_{table_name}_cdc__value",
        "namespace": AVRO_SCHEMA_NAMESPACE,
        "type": "record",
        "fields": AvroSchemaGenerator.get_cdc_metadata_fields_avro_schemas(
            schema_name, table_name, source_value_field_names) + source_value_fields
    }
    target_value_schema_json = {
        "name": f"{schema_name}_{table_name}_cdc__value",
        "namespace": AVRO_SCHEMA_NAMESPACE,
        "type": "record",
        "fields": AvroSchemaGenerator.get_cdc_metadata_fields_avro_schemas(
            schema_name, table_name, target_value_field_names) + target_value_fields
    }

    if source_value_schema_json == target_value_schema_json:
        return True, warnings

    schema_registry = confluent_kafka.avro.CachedSchemaRegistryClient(schema_registry_url)
    target_schema = confluent_kafka.avro.loads(json.dumps(target_value_schema_json))
    subject_name = f'{topic_name}-value'

    is_compatible = schema_registry.test_compatibility(subject_name, target_schema)
    if not is_compatible:
        warnings.append(f'Value schema for topic {topic_name} would NOT be compatible with existing registered schema')

    added_cols = set(target_value_field_names) - set(source_value_field_names)
    removed_cols = set(source_value_field_names) - set(target_value_field_names)
    if added_cols:
        warnings.append(f'  New columns in target: {sorted(added_cols)}')
    if removed_cols:
        warnings.append(f'  Columns removed in target: {sorted(removed_cols)}')

    return is_compatible, warnings


def register_new_schemas(
        schema_registry_url: str, topic_name: str,
        target_columns: List[ColumnMetadata],
        schema_generator: AvroSchemaGenerator, fq_name: str
) -> None:
    schema_name, table_name = fq_name.split('.')
    schema_registry = confluent_kafka.avro.CachedSchemaRegistryClient(schema_registry_url)

    target_key_cols = sorted([c for c in target_columns if c.primary_key_ordinal is not None],
                             key=lambda c: c.primary_key_ordinal)
    target_value_cols = sorted(target_columns, key=lambda c: c.change_table_ordinal)
    target_value_field_names = [c.column_name for c in target_value_cols]

    key_fields = [schema_generator.get_record_field_schema(
        schema_name, table_name, c.column_name, c.sql_type_name,
        c.decimal_precision, c.decimal_scale, False) for c in target_key_cols]
    key_schema_json = {
        "name": f"{schema_name}_{table_name}_cdc__key",
        "namespace": AVRO_SCHEMA_NAMESPACE,
        "type": "record",
        "fields": key_fields
    }
    key_schema = confluent_kafka.avro.loads(json.dumps(key_schema_json))
    schema_registry.register(f'{topic_name}-key', key_schema)

    value_fields = [schema_generator.get_record_field_schema(
        schema_name, table_name, c.column_name, c.sql_type_name,
        c.decimal_precision, c.decimal_scale, True) for c in target_value_cols]
    value_schema_json = {
        "name": f"{schema_name}_{table_name}_cdc__value",
        "namespace": AVRO_SCHEMA_NAMESPACE,
        "type": "record",
        "fields": AvroSchemaGenerator.get_cdc_metadata_fields_avro_schemas(
            schema_name, table_name, target_value_field_names) + value_fields
    }
    value_schema = confluent_kafka.avro.loads(json.dumps(value_schema_json))
    schema_registry.register(f'{topic_name}-value', value_schema)

    logger.info('Registered key and value schemas for topic %s', topic_name)


def snapshot_needs_redo(source_columns: List[ColumnMetadata], target_columns: List[ColumnMetadata],
                        schema_generator: AvroSchemaGenerator, fq_name: str) -> bool:
    schema_name, table_name = fq_name.split('.')
    source_cols_by_name = {c.column_name: c for c in source_columns}
    target_cols_by_name = {c.column_name: c for c in target_columns}

    added = set(target_cols_by_name.keys()) - set(source_cols_by_name.keys())
    removed = set(source_cols_by_name.keys()) - set(target_cols_by_name.keys())

    if removed:
        return True

    for col_name in added:
        if not target_cols_by_name[col_name].is_nullable:
            return True

    for col_name in set(source_cols_by_name.keys()) & set(target_cols_by_name.keys()):
        src = source_cols_by_name[col_name]
        tgt = target_cols_by_name[col_name]
        if src.sql_type_name == tgt.sql_type_name and src.decimal_precision == tgt.decimal_precision \
                and src.decimal_scale == tgt.decimal_scale:
            continue
        old_avro_type = schema_generator.get_record_field_schema(
            schema_name, table_name, col_name, src.sql_type_name,
            src.decimal_precision, src.decimal_scale, True)
        new_avro_type = schema_generator.get_record_field_schema(
            schema_name, table_name, col_name, tgt.sql_type_name,
            tgt.decimal_precision, tgt.decimal_scale, True)
        if old_avro_type != new_avro_type:
            return True

    return False


def build_transfer_plans(
        source_instances: Dict[str, CaptureInstanceMetadata],
        source_columns: Dict[str, List[ColumnMetadata]],
        target_instances: Dict[str, CaptureInstanceMetadata],
        target_columns: Dict[str, List[ColumnMetadata]],
        prior_progress: Dict[Tuple[str, str], progress_tracking.ProgressEntry],
        topic_name_template: str,
        target_db_conn: pyodbc.Connection,
        schema_registry_url: str,
        schema_generator: AvroSchemaGenerator
) -> Tuple[List[TableTransferPlan], List[str]]:
    plans: List[TableTransferPlan] = []
    global_warnings: List[str] = []

    source_tables = set(source_instances.keys())
    target_tables = set(target_instances.keys())

    missing_in_target = source_tables - target_tables
    extra_in_target = target_tables - source_tables

    if missing_in_target:
        global_warnings.append(
            f'Tables present in source but NOT in target: {sorted(missing_in_target)}')
    if extra_in_target:
        global_warnings.append(
            f'Tables present in target but NOT in source (will be ignored): {sorted(extra_in_target)}')

    if '{capture_instance_name}' in topic_name_template:
        global_warnings.append(
            'Topic name template includes {capture_instance_name}. If capture instance names differ '
            'between source and target, topic names will be resolved using the SOURCE capture instance '
            'name (since that is what existing topics/progress were created with).')

    for fq_name in sorted(source_tables & target_tables):
        source_ci = source_instances[fq_name]
        target_ci = target_instances[fq_name]
        warnings: List[str] = []

        schema_name, table_name = fq_name.split('.')
        topic_name = topic_name_template.format(
            schema_name=schema_name, table_name=table_name,
            capture_instance_name=source_ci.capture_instance_name)

        src_cols = source_columns.get(fq_name, [])
        tgt_cols = target_columns.get(fq_name, [])

        source_key_fields = get_key_fields_for_table(src_cols)
        target_key_fields = get_key_fields_for_table(tgt_cols)

        if source_key_fields != target_key_fields:
            warnings.append(f'KEY MISMATCH: source keys {source_key_fields} != target keys {target_key_fields}')

        snapshot_progress_entry = prior_progress.get((topic_name, constants.SNAPSHOT_ROWS_KIND))
        change_progress_entry = prior_progress.get((topic_name, constants.CHANGE_ROWS_KIND))

        prior_change_index = change_progress_entry.change_index if change_progress_entry else None
        prior_snapshot_index = snapshot_progress_entry.snapshot_index if snapshot_progress_entry else None

        new_change_index = get_max_change_index_for_capture_instance(target_db_conn, target_ci.capture_instance_name)
        if new_change_index is None:
            warnings.append(f'Could not determine max LSN for target capture instance {target_ci.capture_instance_name}')

        needs_new_snapshot = snapshot_needs_redo(src_cols, tgt_cols, schema_generator, fq_name)
        if needs_new_snapshot:
            snapshot_action = 'RESET (schema change requires new snapshot)'
            new_snapshot_progress = None
        elif prior_snapshot_index == constants.SNAPSHOT_COMPLETION_SENTINEL:
            snapshot_action = 'COPY (already complete)'
            new_snapshot_progress = constants.SNAPSHOT_COMPLETION_SENTINEL
        elif prior_snapshot_index is not None:
            snapshot_action = 'COPY (in-progress snapshot position)'
            new_snapshot_progress = prior_snapshot_index
        else:
            snapshot_action = 'NO PRIOR PROGRESS'
            new_snapshot_progress = None

        change_action = 'SET to target DB latest position'

        schema_compatible: Optional[bool] = None
        if src_cols and tgt_cols:
            compat, schema_warnings = check_schema_compatibility(
                schema_registry_url, topic_name, src_cols, tgt_cols, schema_generator, fq_name)
            schema_compatible = compat
            warnings.extend(schema_warnings)

        plans.append(TableTransferPlan(
            fq_name=fq_name,
            topic_name=topic_name,
            source_capture_instance=source_ci.capture_instance_name,
            target_capture_instance=target_ci.capture_instance_name,
            source_key_fields=source_key_fields,
            target_key_fields=target_key_fields,
            snapshot_action=snapshot_action,
            change_action=change_action,
            prior_change_progress=prior_change_index,
            new_change_index=new_change_index,
            prior_snapshot_progress=prior_snapshot_index,
            new_snapshot_progress=new_snapshot_progress,
            warnings=warnings,
            schema_compatible=schema_compatible,
        ))

    return plans, global_warnings


def execute_transfer(
        plans: List[TableTransferPlan],
        kafka_client: kafka.KafkaClient,
        serializer: AvroSerializer,
        progress_topic_name: str,
        schema_registry_url: str,
        schema_generator: AvroSchemaGenerator,
        target_columns: Dict[str, List[ColumnMetadata]]
) -> None:
    progress_tracker = progress_tracking.ProgressTracker(
        kafka_client, serializer, progress_topic_name, socket.getfqdn())

    for plan in plans:
        if plan.warnings and any('KEY MISMATCH' in w for w in plan.warnings):
            logger.error('Skipping %s due to key mismatch', plan.fq_name)
            continue

        if plan.schema_compatible is False:
            logger.error('Skipping %s due to schema incompatibility', plan.fq_name)
            continue

        logger.info('Transferring ownership for %s (topic: %s)', plan.fq_name, plan.topic_name)

        fq_change_table_name = helpers.get_fq_change_table_name(plan.target_capture_instance)

        tgt_cols = target_columns.get(plan.fq_name, [])
        if tgt_cols:
            register_new_schemas(schema_registry_url, plan.topic_name, tgt_cols,
                                 schema_generator, plan.fq_name)

        kafka_client.begin_transaction()

        if plan.new_change_index:
            progress_entry = progress_tracking.ProgressEntry(
                progress_kind=constants.CHANGE_ROWS_KIND,
                topic_name=plan.topic_name,
                source_table_name=plan.fq_name,
                change_table_name=fq_change_table_name,
                change_index=plan.new_change_index
            )
            key, value = serializer.serialize_progress_tracking_message(progress_entry)
            kafka_client.produce(
                topic=progress_topic_name, key=key, value=value,
                message_type=constants.CHANGE_PROGRESS_MESSAGE)
            logger.info('  Wrote change progress: %s', plan.new_change_index)

        if plan.new_snapshot_progress is not None:
            progress_entry = progress_tracking.ProgressEntry(
                progress_kind=constants.SNAPSHOT_ROWS_KIND,
                topic_name=plan.topic_name,
                source_table_name=plan.fq_name,
                change_table_name=fq_change_table_name,
                snapshot_index=plan.new_snapshot_progress
            )
            key, value = serializer.serialize_progress_tracking_message(progress_entry)
            kafka_client.produce(
                topic=progress_topic_name, key=key, value=value,
                message_type=constants.SNAPSHOT_PROGRESS_MESSAGE)
            logger.info('  Wrote snapshot progress: %s', plan.snapshot_action)
        elif plan.snapshot_action == 'RESET (schema change requires new snapshot)':
            progress_entry = progress_tracking.ProgressEntry(
                progress_kind=constants.SNAPSHOT_ROWS_KIND,
                topic_name=plan.topic_name,
                source_table_name=plan.fq_name,
                change_table_name=fq_change_table_name,
            )
            key, _ = serializer.serialize_progress_tracking_message(progress_entry)
            kafka_client.produce(
                topic=progress_topic_name, key=key, value=None,
                message_type=constants.PROGRESS_DELETION_TOMBSTONE_MESSAGE)
            logger.info('  Deleted snapshot progress (new snapshot needed)')

        kafka_client.commit_transaction()

    progress_tracking.ProgressTracker._instance = None


def main() -> None:
    p = argparse.ArgumentParser(
        description='Transfer CDC-to-Kafka topic ownership between database instances.')

    p.add_argument('--source-db-conn-string', required=True,
                   default=os.environ.get('SOURCE_DB_CONN_STRING'),
                   help='ODBC connection string for the source (current owner) database')
    p.add_argument('--target-db-conn-string', required=True,
                   default=os.environ.get('TARGET_DB_CONN_STRING'),
                   help='ODBC connection string for the target (new owner) database')
    p.add_argument('--kafka-bootstrap-servers', required=True,
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
                   help='Kafka bootstrap servers')
    p.add_argument('--schema-registry-url', required=True,
                   default=os.environ.get('SCHEMA_REGISTRY_URL'),
                   help='URL to schema registry (required for progress deserialization and schema '
                        'compatibility checks)')
    p.add_argument('--progress-topic-name',
                   default=os.environ.get('PROGRESS_TOPIC_NAME', '_cdc_to_kafka_progress'),
                   help='Name of the progress tracking topic')
    p.add_argument('--topic-name-template',
                   default=os.environ.get('TOPIC_NAME_TEMPLATE', '{schema_name}_{table_name}_cdc'),
                   help='Template for topic names')
    p.add_argument('--table-include-regex',
                   default=os.environ.get('TABLE_INCLUDE_REGEX'),
                   help='Regex to include tables')
    p.add_argument('--table-exclude-regex',
                   default=os.environ.get('TABLE_EXCLUDE_REGEX'),
                   help='Regex to exclude tables')
    p.add_argument('--capture-instance-version-strategy',
                   choices=(options.CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX,
                            options.CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE),
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_STRATEGY',
                                          options.CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE),
                   help='Strategy for selecting capture instance version')
    p.add_argument('--capture-instance-version-regex',
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_REGEX'),
                   help='Regex for capture instance version strategy')
    p.add_argument('--always-use-avro-longs',
                   type=options.str2bool, nargs='?', const=True,
                   default=options.str2bool(os.environ.get('ALWAYS_USE_AVRO_LONGS', '0')),
                   help='Use Avro longs for int types')
    p.add_argument('--avro-type-spec-overrides',
                   default=os.environ.get('AVRO_TYPE_SPEC_OVERRIDES', '{}'), type=json.loads,
                   help='JSON object of Avro type overrides')
    p.add_argument('--extra-kafka-consumer-config',
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG', '{}'), type=json.loads,
                   help='Extra Kafka consumer config as JSON')
    p.add_argument('--extra-kafka-producer-config',
                   default=os.environ.get('EXTRA_KAFKA_PRODUCER_CONFIG', '{}'), type=json.loads,
                   help='Extra Kafka producer config as JSON')
    p.add_argument('--execute', action='store_true', default=False,
                   help='Actually execute the transfer. Without this flag, runs in dry-run mode.')

    args = p.parse_args()

    logger.info('=== CDC-to-Kafka Topic Ownership Transfer Tool ===')
    logger.info('Mode: %s', 'EXECUTE' if args.execute else 'DRY RUN')
    logger.info('')

    source_db_conn = pyodbc.connect(args.source_db_conn_string)
    target_db_conn = pyodbc.connect(args.target_db_conn_string)

    logger.info('Connected to source and target databases.')

    logger.info('Discovering capture instances in source DB...')
    source_instances, source_columns = get_capture_instances_with_columns(
        source_db_conn, args.capture_instance_version_strategy, args.capture_instance_version_regex,
        args.table_include_regex, args.table_exclude_regex)
    logger.info('Found %d capture instances in source DB.', len(source_instances))

    logger.info('Discovering capture instances in target DB...')
    target_instances, target_columns = get_capture_instances_with_columns(
        target_db_conn, args.capture_instance_version_strategy, args.capture_instance_version_regex,
        args.table_include_regex, args.table_exclude_regex)
    logger.info('Found %d capture instances in target DB.', len(target_instances))


    logger.info('Reading existing progress from Kafka topic %s...', args.progress_topic_name)

    with kafka.KafkaClient(accumulator.NoopAccumulator(), args.kafka_bootstrap_servers,
                           args.extra_kafka_consumer_config, args.extra_kafka_producer_config,
                           disable_writing=True) as kafka_client:
        serializer = AvroSerializer(
            args.schema_registry_url, args.always_use_avro_longs, args.progress_topic_name,
            '', '', args.avro_type_spec_overrides, True)
        progress_tracker = progress_tracking.ProgressTracker(
            kafka_client, serializer, args.progress_topic_name, socket.getfqdn())
        prior_progress = progress_tracker.get_prior_progress_or_create_progress_topic()

    kafka.KafkaClient._instance = None
    progress_tracking.ProgressTracker._instance = None

    logger.info('Found %d progress entries.', len(prior_progress))

    AvroSchemaGenerator._instance = None
    schema_generator = AvroSchemaGenerator(args.always_use_avro_longs, args.avro_type_spec_overrides)

    plans, global_warnings = build_transfer_plans(
        source_instances, source_columns, target_instances, target_columns,
        prior_progress, args.topic_name_template, target_db_conn,
        args.schema_registry_url, schema_generator)

    if global_warnings:
        logger.warning('')
        logger.warning('=== GLOBAL WARNINGS ===')
        for w in global_warnings:
            logger.warning('  %s', w)
        logger.warning('')

    table_data = []
    for plan in plans:
        ci_change = ''
        if plan.source_capture_instance != plan.target_capture_instance:
            ci_change = f'{plan.source_capture_instance} -> {plan.target_capture_instance}'
        else:
            ci_change = plan.source_capture_instance

        schema_status = ''
        if plan.schema_compatible is True:
            schema_status = 'OK'
        elif plan.schema_compatible is False:
            schema_status = 'INCOMPATIBLE'
        elif plan.schema_compatible is None:
            schema_status = 'SAME'

        table_data.append((
            plan.fq_name,
            ci_change,
            plan.snapshot_action,
            plan.change_action,
            schema_status,
            '; '.join(plan.warnings) if plan.warnings else ''
        ))

    headers = ('Table', 'Capture Instance', 'Snapshot', 'Change Progress', 'Schema', 'Warnings')
    print('\n' + tabulate(table_data, headers, tablefmt='fancy_grid') + '\n')

    logger.info('')
    logger.info('=== PRIOR PROGRESS VALUES (for backup/revert purposes) ===')
    prior_values_data = []
    for plan in plans:
        prior_values_data.append((
            plan.topic_name,
            repr(plan.prior_change_progress) if plan.prior_change_progress else '<none>',
            str(plan.prior_snapshot_progress) if plan.prior_snapshot_progress else '<none>',
        ))
    prior_headers = ('Topic', 'Prior Change Progress', 'Prior Snapshot Progress')
    print(tabulate(prior_values_data, prior_headers, tablefmt='fancy_grid') + '\n')

    logger.info('=== NEW PROGRESS VALUES (what would be written) ===')
    new_values_data = []
    for plan in plans:
        new_values_data.append((
            plan.topic_name,
            repr(plan.new_change_index) if plan.new_change_index else '<none>',
            str(plan.new_snapshot_progress) if plan.new_snapshot_progress else '<will reset>',
        ))
    new_headers = ('Topic', 'New Change Progress', 'New Snapshot Progress')
    print(tabulate(new_values_data, new_headers, tablefmt='fancy_grid') + '\n')

    has_blocking_issues = False
    for plan in plans:
        if any('KEY MISMATCH' in w for w in plan.warnings):
            has_blocking_issues = True
        if plan.schema_compatible is False:
            has_blocking_issues = True

    if has_blocking_issues:
        logger.error('There are blocking issues (key mismatches or schema incompatibilities). '
                     'These tables will be skipped if --execute is used.')

    if not args.execute:
        logger.info('Dry run complete. Use --execute to apply these changes.')
        source_db_conn.close()
        target_db_conn.close()
        return

    logger.info('Executing transfer...')

    with kafka.KafkaClient(accumulator.NoopAccumulator(), args.kafka_bootstrap_servers,
                           args.extra_kafka_consumer_config, args.extra_kafka_producer_config,
                           disable_writing=False, transactional_id='cdc_to_kafka_transfer_ownership') as kafka_client:
        AvroSchemaGenerator._instance = None
        serializer = AvroSerializer(
            args.schema_registry_url, args.always_use_avro_longs, args.progress_topic_name,
            '', '', args.avro_type_spec_overrides, False)

        execute_transfer(plans, kafka_client, serializer, args.progress_topic_name,
                         args.schema_registry_url, schema_generator, target_columns)

    logger.info('Transfer complete.')
    source_db_conn.close()
    target_db_conn.close()


if __name__ == '__main__':
    main()
