import argparse
import json
import logging
from typing import List, Any, Dict

import confluent_kafka
from tabulate import tabulate

from . import kafka, constants, options
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


RETENTION_CONFIG_NAMES = (
    'retention.ms',
    'retention.bytes',
    'cleanup.policy'
)


def main() -> None:
    def add_args(p: argparse.ArgumentParser) -> None:
        p.add_argument('--topic-names', required=True)
        p.add_argument('--script-output-file', type=argparse.FileType('w'))
        p.add_argument('--extra-kafka-cli-command-arg', nargs='*')

    opts, _, serializer = options.get_options_and_metrics_reporters(add_args)
    print(opts.topic_names)
    topic_names: List[str] = [x.strip() for x in opts.topic_names.strip().split(',') if x.strip()]
    display_table: List[List[str]] = []
    completions_seen_since_start: Dict[str, bool] = {tn: False for tn in topic_names}
    last_starts: Dict[str, Dict[str, Any]] = {tn: {} for tn in topic_names}
    consumed_count: int = 0
    relevant_count: int = 0
    table_headers = [
        "Topic",
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

    with kafka.KafkaClient(accumulator.NoopAccumulator(), opts.kafka_bootstrap_servers,
                           opts.extra_kafka_consumer_config, {}, disable_writing=True) as kafka_client:
        for msg in kafka_client.consume_all(opts.snapshot_logging_topic_name):
            deser_msg = serializer.deserialize(msg)
            log = deser_msg.value_dict or {}
            consumed_count += 1
            if log['topic_name'] not in topic_names:
                continue
            relevant_count += 1
            display_table.append([
                log['topic_name'],
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
                last_starts[log['topic_name']] = log
                completions_seen_since_start[log['topic_name']] = False
            if log["action"] == constants.SNAPSHOT_LOG_ACTION_COMPLETED:
                completions_seen_since_start[log['topic_name']] = True

        print(f'''
Consumed {consumed_count} messages from snapshot logging topic {opts.snapshot_logging_topic_name}.
{relevant_count} were related to requested topics {topic_names}.
        ''')

        if not relevant_count:
            exit(0)

        display_table = sorted(display_table, key=lambda x: (x[0], x[1]))
        print(tabulate(display_table, table_headers))

        watermarks = kafka_client.get_topic_watermarks(topic_names)

        for topic_name in topic_names:
            all_topic_configs = kafka_client.get_topic_config(topic_name)
            topic_has_delete_cleanup_policy = 'delete' in all_topic_configs['cleanup.policy'].value
            topic_level_retention_configs = {
                k: v.value for k, v in all_topic_configs.items()
                if k in RETENTION_CONFIG_NAMES
                and v.source == confluent_kafka.admin.ConfigSource.DYNAMIC_TOPIC_CONFIG.value
            }

            print(f'''
            
----- Topic {topic_name} -----
Current retention-related configs for the topic are: {topic_level_retention_configs}.      
Current topic watermarks are: {watermarks[topic_name]}.''')

            if not last_starts[topic_name]:
                print('No logged snapshot event history was found for this topic.')
            else:
                topic_starts_with_last_snapshot = True
                for part, (low_wm, _) in enumerate(watermarks[topic_name]):
                    if low_wm != last_starts[topic_name]['partition_watermarks_high'][str(part)]:
                        topic_starts_with_last_snapshot = False
                if topic_starts_with_last_snapshot:
                    print('The first message in all topic-partitions appears to coincide with the beginning of the most recent snapshot.')
                else:
                    if not completions_seen_since_start[topic_name]:
                        print('''
--------------------------------------------------------------------------------------------------------------------------------------------------            
*** NOTE: A snapshot completion has not been logged since the most recent start! Take note in considering whether to run the commands below... ***
--------------------------------------------------------------------------------------------------------------------------------------------------
                        ''')
                    delete_parts = [{"topic": topic_name, "partition": int(part), "offset": wm}
                                    for part, wm in last_starts[topic_name]['partition_watermarks_high'].items()]
                    delete_spec = json.dumps({"partitions": delete_parts}, indent=4)
                    if not topic_has_delete_cleanup_policy:
                        extra_args_str = f' --{" --".join(opts.extra_kafka_cli_command_arg)}' if opts.extra_kafka_cli_command_arg else ''
                        config_alter = (f'kafka-configs{extra_args_str} --bootstrap-server {opts.kafka_bootstrap_servers} '
                                        f'--alter --entity-type topics --entity-name {topic_name} --add-config '
                                        f'cleanup.policy=delete,retention.ms=-1,retention.bytes=-1')
                        to_add = []
                        to_delete = []
                        for ret_con in RETENTION_CONFIG_NAMES:
                            if ret_con in topic_level_retention_configs:
                                to_add.append(f'{ret_con}=[{topic_level_retention_configs[ret_con]}]')
                            else:
                                to_delete.append(ret_con)
                        restore_by_add = ''
                        if to_add:
                            restore_by_add = (f"kafka-configs{extra_args_str} --bootstrap-server {opts.kafka_bootstrap_servers} --alter "
                                              f"--entity-type topics --entity-name {topic_name} "
                                              f"--add-config {','.join(to_add)}")
                        restore_by_delete = ''
                        if to_delete:
                            restore_by_delete = (f"kafka-configs{extra_args_str} --bootstrap-server {opts.kafka_bootstrap_servers} --alter "
                                                 f"--entity-type topics --entity-name {topic_name} "
                                                 f"--delete-config {','.join(to_delete)}")

                        commands = f'''
{config_alter}
cat << "EOF" > delete-records-{topic_name}.json
{delete_spec}
EOF
kafka-delete-records{extra_args_str} --bootstrap-server {opts.kafka_bootstrap_servers} --offset-json-file delete-records-{topic_name}.json
{restore_by_add}
{restore_by_delete}
                        '''
                        print(f'''The following sequence of Kafka CLI tool commands would allow you to delete all messages in the topic prior to the beginning of the most recent snapshot:
                        
{commands}''')
                        if opts.script_output_file:
                            opts.script_output_file.write(commands)


if __name__ == "__main__":
    # importing this file to pick up the logging config in __init__; is there a better way??
    # noinspection PyUnresolvedReferences
    from cdc_kafka import show_snapshot_history
    show_snapshot_history.main()
