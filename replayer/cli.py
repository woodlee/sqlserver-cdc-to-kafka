import argparse
import json
import os
from typing import List

from .logging_config import get_logger
from .models import ReplayConfig
from .modes import run_backfill_mode, run_follow_mode

logger = get_logger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(description='Replays CDC-to-Kafka topics to SQL Server tables.')

    # Config for data source
    p.add_argument('--replay-topic',
                   default=os.environ.get('REPLAY_TOPIC'),
                   help='Single topic to replay (for backward compatibility)')
    p.add_argument('--topic-to-table-map',
                   default=os.environ.get('TOPIC_TO_TABLE_MAP'),
                   help='JSON mapping of topics to tables, e.g. {"topic1": {"schema": "dbo", "table": "Table1"}, ...}')
    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))
    p.add_argument('--extra-kafka-consumer-config',
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG', {}), type=json.loads)

    # Config for data target / progress tracking
    p.add_argument('--target-db-server',
                   default=os.environ.get('TARGET_DB_SERVER'))
    p.add_argument('--target-db-user',
                   default=os.environ.get('TARGET_DB_USER'))
    p.add_argument('--target-db-password',
                   default=os.environ.get('TARGET_DB_PASSWORD'))
    p.add_argument('--target-db-database',
                   default=os.environ.get('TARGET_DB_DATABASE'))
    p.add_argument('--target-db-table-schema',
                   default=os.environ.get('TARGET_DB_TABLE_SCHEMA'),
                   help='Single table schema (for backward compatibility)')
    p.add_argument('--target-db-table-name',
                   default=os.environ.get('TARGET_DB_TABLE_NAME'),
                   help='Single table name (for backward compatibility)')
    p.add_argument('--cols-to-not-sync',
                   default=os.environ.get('COLS_TO_NOT_SYNC', ''))
    p.add_argument('--primary-key-fields-override',
                   default=os.environ.get('PRIMARY_KEY_FIELDS_OVERRIDE', ''))
    p.add_argument('--progress-tracking-namespace',
                   default=os.environ.get('PROGRESS_TRACKING_NAMESPACE', 'default'))
    p.add_argument('--progress-tracking-table-schema',
                   default=os.environ.get('PROGRESS_TRACKING_TABLE_SCHEMA', 'dbo'))
    p.add_argument('--progress-tracking-table-name',
                   default=os.environ.get('PROGRESS_TRACKING_TABLE_NAME', 'CdcKafkaReplayerProgress'))

    # Config for process behavior and tuning
    p.add_argument('--delete-batch-size', type=int,
                   default=os.environ.get('DELETE_BATCH_SIZE', 2000))
    p.add_argument('--upsert-batch-size', type=int,
                   default=os.environ.get('UPSERT_BATCH_SIZE', 5000))
    p.add_argument('--max-commit-latency-seconds', type=int,
                   default=os.environ.get('MAX_COMMIT_LATENCY_SECONDS', 10))
    p.add_argument('--consumed-messages-limit', type=int,
                   default=os.environ.get('CONSUMED_MESSAGES_LIMIT', 0))
    p.add_argument('--truncate-existing-data', action='store_true',
                   default=os.environ.get('TRUNCATE_EXISTING_DATA', '').lower() in ('true', '1', 'yes'),
                   help='Truncate target table data if no prior progress exists')

    # Mode selection for backfill vs follow
    p.add_argument('--mode',
                   default=os.environ.get('REPLAYER_MODE', 'backfill'),
                   choices=['backfill', 'follow'],
                   help='Operation mode: "backfill" replays single-table topics in parallel up to a cutoff LSN, '
                        '"follow" reads the all-changes topic in order to maintain FK constraints')
    p.add_argument('--all-changes-topic',
                   default=os.environ.get('ALL_CHANGES_TOPIC'),
                   help='Name of the unified all-changes topic (e.g., "ssy_all_cdc_changes") containing messages '
                        'from all tables in LSN order')

    opts, _ = p.parse_known_args()

    if not (opts.kafka_bootstrap_servers and opts.schema_registry_url and opts.target_db_server and
            opts.target_db_user and opts.target_db_password and opts.target_db_database):
        raise Exception('Arguments kafka_bootstrap_servers, schema_registry_url, target_db_server, '
                        'target_db_user, target_db_password, and target_db_database are all required.')

    if not opts.all_changes_topic:
        raise Exception('Argument --all-changes-topic is required.')

    replay_configs: List[ReplayConfig] = []

    if opts.topic_to_table_map:
        topic_map = json.loads(opts.topic_to_table_map)
        for topic, table_info in topic_map.items():
            config = ReplayConfig(
                replay_topic=topic,
                target_db_table_schema=table_info.get('schema', 'dbo'),
                target_db_table_name=table_info['table'],
                cols_to_not_sync=table_info.get('cols_to_not_sync', ''),
                primary_key_fields_override=table_info.get('primary_key_fields_override', '')
            )
            replay_configs.append(config)
    elif opts.replay_topic and opts.target_db_table_schema and opts.target_db_table_name:
        config = ReplayConfig(
            replay_topic=opts.replay_topic,
            target_db_table_schema=opts.target_db_table_schema,
            target_db_table_name=opts.target_db_table_name,
            cols_to_not_sync=opts.cols_to_not_sync,
            primary_key_fields_override=opts.primary_key_fields_override
        )
        replay_configs.append(config)
    else:
        raise Exception('Either --topic-to-table-map OR (--replay-topic, --target-db-table-schema, '
                        'and --target-db-table-name) must be provided.')

    logger.info(f"Starting CDC replayer in {opts.mode} mode with {len(replay_configs)} topic(s).")

    if opts.mode == 'backfill':
        run_backfill_mode(opts, replay_configs)
    else:
        run_follow_mode(opts, replay_configs)


if __name__ == '__main__':
    main()
