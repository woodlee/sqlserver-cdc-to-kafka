import argparse
import collections
import copy
import datetime
import logging
import os
import re
from typing import Dict, Optional, Set

import confluent_kafka
from tabulate import tabulate

from cdc_kafka import kafka, constants, progress_tracking, options, helpers
from .metric_reporting import accumulator

logger = logging.getLogger(__name__)


class TopicProgressInfo(object):
    def __init__(self) -> None:
        self.change_progress_count: int = 0
        self.snapshot_progress_count: int = 0
        self.last_change_progress: Optional[confluent_kafka.Message] = None
        self.last_snapshot_progress: Optional[confluent_kafka.Message] = None
        self.distinct_change_tables: Set[str] = set()
        self.reset_count: int = 0
        self.evolution_count: int = 0
        self.heartbeat_count: int = 0
        self.problem_count: int = 0


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--topics-to-include-regex',
                   default=os.environ.get('TOPICS_TO_INCLUDE_REGEX', '.*'))
    p.add_argument('--topics-to-exclude-regex',
                   default=os.environ.get('TOPICS_TO_EXCLUDE_REGEX'))
    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'))
    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'))
    p.add_argument('--progress-topic-name',
                   default=os.environ.get('PROGRESS_TOPIC_NAME', '_cdc_to_kafka_progress'))
    p.add_argument('--show-all',
                   type=options.str2bool, nargs='?', const=True,
                   default=options.str2bool(os.environ.get('SHOW_ALL', '0')))
    opts = p.parse_args()

    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers):
        raise Exception('Arguments schema_registry_url and kafka_bootstrap_servers are required.')

    with kafka.KafkaClient(accumulator.NoopAccumulator(), opts.kafka_bootstrap_servers,
                           opts.schema_registry_url, {}, {}, disable_writing=True) as kafka_client:
        if kafka_client.get_topic_partition_count(opts.progress_topic_name) is None:
            logger.error('Progress topic %s not found.', opts.progress_topic_name)
            exit(1)

        topic_include_re = re.compile(opts.topics_to_include_regex, re.IGNORECASE)
        topic_exclude_re = None
        if opts.topics_to_exclude_regex:
            topic_exclude_re = re.compile(opts.topics_to_exclude_regex, re.IGNORECASE)

        msg_ctr = 0
        topic_info: Dict[str, TopicProgressInfo] = collections.defaultdict(TopicProgressInfo)

        for msg in kafka_client.consume_all(opts.progress_topic_name):
            if not msg_ctr:
                logger.info('Read first message: %s', helpers.format_coordinates(msg))

            msg_ctr += 1

            if msg_ctr % 100000 == 0:
                logger.info('Read %s messages so far; last was %s', msg_ctr, helpers.format_coordinates(msg))

            # noinspection PyTypeChecker
            msg_key = dict(msg.key())
            topic, kind = msg_key['topic_name'], msg_key['progress_kind']

            if not topic_include_re.match(topic):
                continue
            if topic_exclude_re and topic_exclude_re.match(topic):
                continue

            prior = copy.copy(topic_info.get(topic))

            # noinspection PyArgumentList
            if msg.value() is None:
                logger.warning('%s progress for topic %s reset at %s', kind, topic, helpers.format_coordinates(msg))
                topic_info[topic].reset_count += 1
                continue

            # noinspection PyTypeChecker,PyArgumentList
            current_change_table = dict(msg.value())['change_table_name']
            topic_info[topic].distinct_change_tables.add(current_change_table)
            current_pe = progress_tracking.ProgressEntry.from_message(msg)

            if kind == constants.CHANGE_ROWS_KIND:
                if not current_pe.change_index:
                    raise Exception('Unexpected state.')
                current_change_index = current_pe.change_index
                topic_info[topic].change_progress_count += 1
                topic_info[topic].last_change_progress = msg
                if current_change_index.is_probably_heartbeat:
                    topic_info[topic].heartbeat_count += 1

                if prior and prior.last_change_progress:
                    prior_pe = progress_tracking.ProgressEntry.from_message(prior.last_change_progress)
                    if not prior_pe.change_index:
                        raise Exception('Unexpected state.')
                    prior_change_index = prior_pe.change_index
                    if prior_change_index == current_change_index and not \
                            current_change_index.is_probably_heartbeat:
                        topic_info[topic].problem_count += 1
                        logger.warning('Duplicate change entry for topic %s between %s and %s', topic,
                                       helpers.format_coordinates(prior.last_change_progress),
                                       helpers.format_coordinates(msg))
                    if prior_change_index > current_change_index:
                        topic_info[topic].problem_count += 1
                        log_msg = '''
Unordered change entry for topic %s
    Prior  : progress message %s, index %s
    Current: progress message %s, index %s
'''
                        logger.error(log_msg, topic, helpers.format_coordinates(prior.last_change_progress),
                                     prior_change_index, helpers.format_coordinates(msg), current_change_index)

            if kind == constants.SNAPSHOT_ROWS_KIND:
                if not current_pe.snapshot_index:
                    raise Exception('Unexpected state.')
                current_snapshot_index = current_pe.snapshot_index
                topic_info[topic].snapshot_progress_count += 1
                topic_info[topic].last_snapshot_progress = msg

                if prior and prior.last_snapshot_progress:
                    prior_pe = progress_tracking.ProgressEntry.from_message(prior.last_snapshot_progress)
                    if not prior_pe.snapshot_index:
                        raise Exception('Unexpected state.')
                    prior_snapshot_index = prior_pe.snapshot_index
                    if current_snapshot_index == constants.SNAPSHOT_COMPLETION_SENTINEL:
                        pass
                    elif prior_snapshot_index == constants.SNAPSHOT_COMPLETION_SENTINEL or \
                            tuple(prior_snapshot_index.values()) < tuple(current_snapshot_index.values()):
                        if prior_pe.change_table_name != current_change_table:
                            topic_info[topic].evolution_count += 1
                            logger.info('Snapshot restart due to schema evolution to capture instance %s for topic %s '
                                        'at progress message %s', current_change_table, topic,
                                        helpers.format_coordinates(msg))
                        else:
                            topic_info[topic].problem_count += 1
                            log_msg = '''
Unordered snapshot entry for topic %s
    Prior  : progress message %s, index %s
    Current: progress message %s, index %s
'''
                            logger.error(log_msg, topic, helpers.format_coordinates(prior.last_snapshot_progress),
                                         prior_pe.snapshot_index, helpers.format_coordinates(msg),
                                         current_pe.snapshot_index)

        logger.info('Read last message: %s', helpers.format_coordinates(msg))

        headers = ('Topic',
                   'Change entries',
                   'Snapshot entries',
                   'Snapshot complete',
                   'Change tables',
                   'Last snapshot progress',
                   'Last change progress',
                   'Progress resets',
                   'Problems',
                   'Evolution re-snaps',
                   'Heartbeat entries')

        table = [[k,
                  v.change_progress_count,
                  v.snapshot_progress_count,
                  'yes' if (progress_tracking.ProgressEntry.from_message(v.last_snapshot_progress).snapshot_index ==
                            constants.SNAPSHOT_COMPLETION_SENTINEL) else 'no',
                  len(v.distinct_change_tables),
                  datetime.datetime.fromtimestamp(v.last_snapshot_progress.timestamp()[1] / 1000,
                                                  datetime.UTC) if v.last_snapshot_progress else None,
                  datetime.datetime.fromtimestamp(v.last_change_progress.timestamp()[1] / 1000,
                                                  datetime.UTC) if v.last_change_progress else None,
                  v.reset_count,
                  v.problem_count,
                  v.evolution_count,
                  v.heartbeat_count]
                 for k, v in topic_info.items() if (
                     opts.show_all or
                     len(v.distinct_change_tables) > 1 or
                     v.reset_count > 0 or
                     v.problem_count > 0 or
                     v.evolution_count > 0
                 )]

        if not opts.show_all:
            logger.warning('Only showing topics with anomalies. Use --show-all to see all topics.')

        if not table:
            logger.warning('No topics to show.')
        else:
            table = sorted(table)
            print(tabulate(table, headers, tablefmt='fancy_grid'))

        logger.info('Progress data parsed for %s topic(s). Check above for possible warnings.', len(topic_info))
        logger.info('Checked %s progress messages from topic %s', msg_ctr, opts.progress_topic_name)


if __name__ == "__main__":
    # importing this file to pick up the logging config in __init__; is there a better way??
    # noinspection PyUnresolvedReferences
    from cdc_kafka import progress_topic_validator
    progress_topic_validator.main()
