import argparse
import datetime
import logging
import os
import re
from typing import Dict, Tuple

from cdc_kafka import kafka, constants, progress_tracking, options
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


class NoopAccumulator(accumulator.AccumulatorAbstract):
    pass


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--topics-to-check-regex',
                   default=os.environ.get('TOPICS_TO_CHECK_REGEX', '.*'))
    p.add_argument('--topics-to-blacklist-regex',
                   default=os.environ.get('TOPICS_TO_BLACKLIST_REGEX'))
    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))
    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--progress-topic-name',
                   default=os.environ.get('PROGRESS_TOPIC_NAME', '_cdc_to_kafka_progress'))
    p.add_argument('--show-all',
                   type=options.str2bool, nargs='?', const=True,
                   default=options.str2bool(os.environ.get('SHOW_ALL', False)))
    opts = p.parse_args()

    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers):
        raise Exception('Arguments schema_registry_url and kafka_bootstrap_servers are required.')

    with kafka.KafkaClient(NoopAccumulator(), opts.kafka_bootstrap_servers,
                           opts.schema_registry_url, {}, {}) as kafka_client:
        kafka_client.register_schemas(opts.progress_topic_name, progress_tracking.PROGRESS_TRACKING_AVRO_KEY_SCHEMA,
                                      progress_tracking.PROGRESS_TRACKING_AVRO_VALUE_SCHEMA)

        result: Dict[Tuple[str, str], Tuple[progress_tracking.ProgressEntry, int, int, str, datetime.datetime]] = {}

        if kafka_client.get_topic_partition_count(opts.progress_topic_name) is None:
            logger.error('Progress topic %s not found.', opts.progress_topic_name)
            exit(1)

        topic_match_re = re.compile(opts.topics_to_check_regex, re.IGNORECASE)
        topic_blacklist_re = None
        if opts.topics_to_blacklist_regex:
            topic_blacklist_re = re.compile(opts.topics_to_blacklist_regex, re.IGNORECASE)

        progress_msg_ctr = 0
        for progress_msg in kafka_client.consume_all(opts.progress_topic_name):
            if progress_msg_ctr % 100000 == 0:
                logger.info('Read %s messages so far...', progress_msg_ctr)

            progress_msg_ctr += 1

            msg_key = dict(progress_msg.key())
            topic, kind = msg_key['topic_name'], msg_key['progress_kind']
            result_key = (topic, kind)
            if not topic_match_re.match(topic):
                continue
            if topic_blacklist_re and topic_blacklist_re.match(topic):
                continue

            if progress_msg.value() is None:
                logger.warning('Progress for %s reset at %s partition %s, offset %s, time %s', result_key,
                               opts.progress_topic_name, progress_msg.partition(), progress_msg.offset(),
                               progress_msg.timestamp())
                if result_key in result:
                    del result[result_key]
                continue

            current_entry = progress_tracking.ProgressEntry(message=progress_msg)
            current_timestamp = datetime.datetime.fromtimestamp(progress_msg.timestamp()[1] / 1000)
            current_ack_part = '-' if current_entry.last_ack_partition is None else current_entry.last_ack_partition
            current_ack_offset = 'Heartbt' if current_entry.last_ack_offset is None \
                else current_entry.last_ack_offset
            current_change_table = dict(progress_msg.value())['change_table_name']

            if result_key in result:
                prior_entry, prior_partition, prior_offset, prior_change_table, prior_time = result[result_key]
                prior_ack_part = '-' if prior_entry.last_ack_partition is None else prior_entry.last_ack_partition
                prior_ack_offset = 'Heartbt' if prior_entry.last_ack_offset is None \
                    else prior_entry.last_ack_offset

                if kind == constants.CHANGE_ROWS_KIND and prior_entry.change_index > current_entry.change_index:
                    logger.error('''Unordered change entry for topic %s
  Prior  : progress partition %s offset %s time %s, ack partition %s offset %s, position %s
  Current: progress partition %s offset %s time %s, ack partition %s offset %s, position %s
  ''', topic, prior_partition, prior_offset, prior_time, prior_ack_part, prior_ack_offset,
                                 prior_entry.change_index, progress_msg.partition(), progress_msg.offset(),
                                 current_timestamp, current_ack_part, current_ack_offset, current_entry.change_index)

                if kind == constants.SNAPSHOT_ROWS_KIND and tuple(prior_entry.snapshot_index.values()) < \
                        tuple(current_entry.snapshot_index.values()):
                    if prior_change_table != current_change_table:
                        logger.info('Snapshot restart due to schema evolution to capture instance %s for topic %s at '
                                    'progress partition %s offset %s time %s', current_change_table, topic,
                                    progress_msg.partition(), progress_msg.offset(), current_timestamp)
                    else:
                        logger.error('''Unordered snapshot entry for topic %s
  Prior  : progress partition %s offset %s time %s, ack partition %s offset %s, position %s
  Current: progress partition %s offset %s time %s, ack partition %s offset %s, position %s
''', topic, prior_partition, prior_offset, prior_time, prior_ack_part, prior_ack_offset,
                                     prior_entry.snapshot_index, progress_msg.partition(), progress_msg.offset(),
                                     current_timestamp, current_ack_part, current_ack_offset,
                                     current_entry.snapshot_index)

            if opts.show_all:
                logger.info('%s %s %s %s %s %s %s %s', topic, kind, progress_msg.partition(), progress_msg.offset(),
                            current_timestamp, current_ack_part, current_ack_offset,
                            current_entry.snapshot_index or current_entry.change_index)

            result[result_key] = (current_entry, progress_msg.partition(), progress_msg.offset(),
                                  current_change_table, current_timestamp)

        logger.info('Checked %s progress messages from topic %s', progress_msg_ctr, opts.progress_topic_name)


if __name__ == "__main__":
    # noinspection PyUnresolvedReferences
    from cdc_kafka import progress_topic_validator  # so we pick up the logging config in __init__; better way??
    progress_topic_validator.main()
