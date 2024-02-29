import argparse
import json
import logging
import os
from typing import List, Optional, Any, Dict

import confluent_kafka
from tabulate import tabulate

from cdc_kafka import kafka
from . import constants
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


RETENTION_CONFIG_NAMES = (
    'retention.ms',
    'retention.bytes',
    'cleanup.policy'
)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--topic-name', required=True,
                   default=os.environ.get('TOPIC_NAME'))
    p.add_argument('--schema-registry-url', required=True,
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))
    p.add_argument('--kafka-bootstrap-servers', required=True,
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--snapshot-logging-topic-name', required=True,
                   default=os.environ.get('SNAPSHOT_LOGGING_TOPIC_NAME'))
    opts = p.parse_args()

    with kafka.KafkaClient(accumulator.NoopAccumulator(), opts.kafka_bootstrap_servers, opts.schema_registry_url, {},
                           {}, disable_writing=True) as kafka_client:
        last_start: Optional[Dict[str, Any]] = None
        consumed_count: int = 0
        relevant_count: int = 0
        headers = [
            "Time",
            "Action",
            "Source table",
            "Starting Key",
            "Ending key",
            "Low watermarks",
            "High watermarks",
            "Acting hostname",
            "Key schema ID",
            "Value schema ID",
        ]
        display_table: List[List[str]] = []
        completion_seen_since_start: bool = False
        for msg in kafka_client.consume_all(opts.snapshot_logging_topic_name):
            # noinspection PyTypeChecker,PyArgumentList
            log = dict(msg.value())
            consumed_count += 1
            if log['topic_name'] != opts.topic_name:
                continue
            relevant_count += 1
            display_table.append([
                log["event_time_utc"],
                log["action"],
                log["table_name"],
                log["starting_snapshot_index"],
                log["ending_snapshot_index"],
                log["partition_watermarks_low"],
                log["partition_watermarks_high"],
                log["process_hostname"],
                log["key_schema_id"],
                log["value_schema_id"],
            ])
            if log["action"] == constants.SNAPSHOT_LOG_ACTION_STARTED:
                last_start = log
                completion_seen_since_start = False
            if log["action"] == constants.SNAPSHOT_LOG_ACTION_COMPLETED:
                completion_seen_since_start = True

        all_topic_configs = kafka_client.get_topic_config(opts.topic_name)
        topic_has_delete_cleanup_policy = 'delete' in all_topic_configs['cleanup.policy'].value
        topic_level_retention_configs = {
            k: v.value for k, v in all_topic_configs.items()
            if k in RETENTION_CONFIG_NAMES
            and v.source == confluent_kafka.admin.ConfigSource.DYNAMIC_TOPIC_CONFIG.value
        }
        watermarks = kafka_client.get_topic_watermarks([opts.topic_name])[opts.topic_name]

    print(f'''
Consumed {consumed_count} messages from snapshot logging topic {opts.snapshot_logging_topic_name}.
{relevant_count} were related to requested topic {opts.topic_name}.
    ''')

    if relevant_count:
        print(f'''
{tabulate(display_table, headers)}

Current retention-related configs for the topic are: {topic_level_retention_configs}.
        
Current topic watermarks are: {watermarks}.
        ''')

    if last_start:
        config_alter, restore_by_add, restore_by_delete = '', '', ''
        delete_parts = [{"topic": opts.topic_name, "partition": int(part), "offset": wm}
                        for part, wm in last_start['partition_watermarks_high'].items()]
        delete_spec = json.dumps({"partitions": delete_parts}, indent=4)
        if not topic_has_delete_cleanup_policy:
            config_alter = (f'kafka-configs --bootstrap-server {opts.kafka_bootstrap_servers} '
                            f'--alter --entity-type topics --entity-name {opts.topic_name} --add-config '
                            f'cleanup.policy=delete,retention.ms=-1,retention.bytes=-1')
            to_add = []
            to_delete = []
            for ret_con in RETENTION_CONFIG_NAMES:
                if ret_con in topic_level_retention_configs:
                    to_add.append(f'{ret_con}=[{topic_level_retention_configs[ret_con]}]')
                else:
                    to_delete.append(ret_con)
            restore_by_add = (f"kafka-configs --bootstrap-server {opts.kafka_bootstrap_servers} --alter "
                              f"--entity-type topics --entity-name {opts.topic_name} "
                              f"--add-config {','.join(to_add)}")
            restore_by_delete = (f"kafka-configs --bootstrap-server {opts.kafka_bootstrap_servers} --alter "
                                 f"--entity-type topics --entity-name {opts.topic_name} "
                                 f"--delete-config {','.join(to_delete)}")

        if not completion_seen_since_start:
            print('''

--------------------------------------------------------------------------------------------------------------------------------------------------            
*** NOTE: A snapshot completion has not been logged since the most recent start! Take note in considering whether to run the commands below... ***
--------------------------------------------------------------------------------------------------------------------------------------------------

            ''')

        print(f'''        
The following sequence of Kafka CLI tool commands would allow you to delete all messages in the topic prior to the beginning of the most recent snapshot:

{config_alter}
cat << "EOF" > delete-records-{opts.topic_name}.json
{delete_spec}
EOF
kafka-delete-records --bootstrap-server {opts.kafka_bootstrap_servers} --offset-json-file delete-records-{opts.topic_name}.json
{restore_by_add}
{restore_by_delete}
        ''')


if __name__ == "__main__":
    # importing this file to pick up the logging config in __init__; is there a better way??
    # noinspection PyUnresolvedReferences
    from cdc_kafka import show_snapshot_history
    show_snapshot_history.main()
