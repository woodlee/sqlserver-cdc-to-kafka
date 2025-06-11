import logging
from typing import List, Dict

from tabulate import tabulate

from . import kafka, constants, options
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


def main() -> None:
    opts, _, serializer = options.get_options_and_metrics_reporters()

    with kafka.KafkaClient(accumulator.NoopAccumulator(), opts.kafka_bootstrap_servers,
                           opts.extra_kafka_consumer_config, {}, disable_writing=True) as kafka_client:
        display_table: List[List[str]] = []
        consumed_count: int = 0
        table_headers = [
            "Topic",
            "Partition",
            "Last start offset",
            "Last completion offset",
            "Current lowest offset",
            "Needs truncation"
        ]

        starts: Dict[str, Dict[str, int]] = {}
        completions: Dict[str, Dict[str, int]] = {}

        for msg in kafka_client.consume_all(opts.snapshot_logging_topic_name):
            deser_msg = serializer.deserialize(msg)
            log = deser_msg.value_dict or {}
            consumed_count += 1
            if log["action"] == constants.SNAPSHOT_LOG_ACTION_STARTED:
                starts[log['topic_name']] = log["partition_watermarks_high"]
            elif log["action"] == constants.SNAPSHOT_LOG_ACTION_COMPLETED:
                completions[log['topic_name']] = log["partition_watermarks_high"]
            else:
                continue

        topic_names = list(starts.keys())
        current_watermarks = kafka_client.get_topic_watermarks(topic_names)

        for topic_name, offsets_by_part in starts.items():
            if topic_name not in current_watermarks:
                continue  # topic was deleted, though there is still snapshot history recorded for it
            for part, snapshot_start_offset in offsets_by_part.items():
                current_low_watermark_offset = current_watermarks[topic_name][int(part)][0]
                last_completion_offset = completions.get(topic_name, {}).get(part, None)

                if last_completion_offset is None or last_completion_offset < snapshot_start_offset:
                    needs_truncation = '!NOT COMPLETE!'
                elif current_low_watermark_offset >= snapshot_start_offset:
                    needs_truncation = 'No'
                else:
                    needs_truncation = 'Yes'

                display_table.append([
                    topic_name,
                    part,
                    snapshot_start_offset,
                    last_completion_offset,
                    current_low_watermark_offset,
                    needs_truncation
                ])

        print(f'Consumed {consumed_count} messages from snapshot logging topic {opts.snapshot_logging_topic_name}.')
        display_table = sorted(display_table, key=lambda x: (x[5], x[0], x[1]))
        print(tabulate(display_table, table_headers))


if __name__ == "__main__":
    # importing this file to pick up the logging config in __init__; is there a better way??
    # noinspection PyUnresolvedReferences
    from cdc_kafka import find_truncation_opportunities

    find_truncation_opportunities.main()
