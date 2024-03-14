import argparse
import json
import logging
import os
import socket

from cdc_kafka import kafka, constants, progress_tracking, options, kafka_oauth
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--topic-names', required=True,
                   default=os.environ.get('TOPIC_NAMES'))
    p.add_argument('--progress-kind', required=True,
                   choices=(constants.CHANGE_ROWS_KIND, constants.ALL_PROGRESS_KINDS, constants.SNAPSHOT_ROWS_KIND),
                   default=os.environ.get('PROGRESS_KIND'))
    p.add_argument('--schema-registry-url', required=True,
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))
    p.add_argument('--kafka-bootstrap-servers', required=True,
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--progress-topic-name', required=True,
                   default=os.environ.get('PROGRESS_TOPIC_NAME'))
    p.add_argument('--snapshot-logging-topic-name', required=True,
                   default=os.environ.get('SNAPSHOT_LOGGING_TOPIC_NAME'))
    p.add_argument('--execute',
                   type=options.str2bool, nargs='?', const=True,
                   default=options.str2bool(os.environ.get('EXECUTE', '0')))
    p.add_argument('--extra-kafka-producer-config',
                   default=os.environ.get('EXTRA_KAFKA_PRODUCER_CONFIG', {}), type=json.loads)
    p.add_argument('--extra-kafka-consumer-config',
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG', {}), type=json.loads)
    kafka_oauth.add_kafka_oauth_arg(p)
    opts, _ = p.parse_known_args()

    logger.info(f"""
    
Progress reset tool: 

WILL {'NOT (because --execute is not set)' if not opts.execute else ''} reset {opts.progress_kind} progress
for topic(s) {opts.topic_names}, if prior progress is found
in progress topic {opts.progress_topic_name} 
in Kafka cluster with bootstrap server(s) {opts.kafka_bootstrap_servers}

Reading progress topic, please wait...

    """)

    with kafka.KafkaClient(accumulator.NoopAccumulator(), opts.kafka_bootstrap_servers, opts.schema_registry_url,
                           opts.extra_kafka_consumer_config, opts.extra_kafka_producer_config,
                           disable_writing=True) as kafka_client:
        progress_tracker = progress_tracking.ProgressTracker(kafka_client, opts.progress_topic_name, socket.getfqdn(),
                                                             opts.snapshot_logging_topic_name)
        progress_entries = progress_tracker.get_prior_progress()

        def act(topic: str, progress_kind: str) -> None:
            if (topic, progress_kind) not in progress_entries:
                logger.warning(f'No {progress_kind} progress found for topic {topic}')
                return
            progress = progress_entries[(topic, progress_kind)]
            logger.info(f'Existing {progress_kind} progress found for topic {topic} at '
                        f'{progress.progress_msg_coordinates}: {progress}')
            if opts.execute:
                kafka_client._disable_writing = False
                progress_tracker.reset_progress(topic, progress_kind, progress.source_table_name, False,
                                                progress.snapshot_index)

        for topic_name in opts.topic_names.split(','):
            topic_name = topic_name.strip()
            if opts.progress_kind == constants.ALL_PROGRESS_KINDS:
                act(topic_name, constants.CHANGE_ROWS_KIND)
                act(topic_name, constants.SNAPSHOT_ROWS_KIND)
            else:
                act(topic_name, opts.progress_kind)


if __name__ == "__main__":
    # importing this file to pick up the logging config in __init__; is there a better way??
    # noinspection PyUnresolvedReferences
    from cdc_kafka import progress_reset_tool
    progress_reset_tool.main()
