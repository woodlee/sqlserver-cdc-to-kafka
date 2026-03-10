import argparse
import logging
import queue as stdlib_queue
import time
from datetime import datetime
from multiprocessing.synchronize import Event as EventClass
from typing import Any, Dict, List, Set

from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import TopicMetadata
from faster_fifo import Queue

from .logging_config import get_logger
from .progress import ProgressTracker
from .kafka_utils import build_consumer_config, commit_cb, format_coordinates
from .models import OrderedOperation, Progress, ReplayConfig
from .table_metadata import FollowModeTableMetadata

logger = get_logger(__name__)


def flush_ordered_operations(db_conn: Any, progress_tracker: ProgressTracker,
                             ops: List[OrderedOperation],
                             table_metadata: Dict[str, FollowModeTableMetadata],
                             last_offset: int, last_timestamp: datetime) -> None:
    """Flush ordered operations and progress update atomically in a single transaction.

    This ensures exactly-once semantics: data operations and progress tracking are
    committed together, so on restart we won't re-process already-committed messages.
    """
    if not ops:
        return

    cursor = db_conn.cursor()
    overall_start = time.perf_counter()

    try:
        # Execute all data operations
        for op in ops:
            metadata = table_metadata[op.original_topic]

            start = time.perf_counter()
            if op.cdc_operation == 'Delete':
                cursor.execute(metadata.single_delete_stmt, tuple(op.key_val))
            elif op.cdc_operation == 'Insert':
                cursor.execute(metadata.insert_stmt, tuple(op.row_values))
            elif op.cdc_operation == 'PostUpdate':
                # Use targeted UPDATE if we have updated_fields info, otherwise fall back to full UPDATE
                if op.updated_fields:
                    stmt, params = metadata.build_dynamic_update(op.row_values, op.updated_fields)
                    cursor.execute(stmt, params)
                else:
                    params = metadata.build_update_params(op.row_values)
                    cursor.execute(metadata.update_stmt, params)
                # if cursor.rowcount == 0:
                #     raise Exception(f'UPDATE affected 0 rows for {metadata.fq_target_table_name} '
                #                    f'with key {op.key_val} - row does not exist')
            else:
                raise Exception(f'Unexpected CDC operation type: {op.cdc_operation}')
            elapsed_us = (time.perf_counter() - start) * 1_000_000
            if elapsed_us > 100_000:
                logger.warning(f"SLOW: {elapsed_us:.0f}µs - {op.cdc_operation} on {op.original_topic}")
        overall_elapsed = time.perf_counter() - overall_start

        logger.info(f'Executed {len(ops)} ops in {overall_elapsed:.2f}s, mean {(overall_elapsed * 1_000 / len(ops)):.2f} ms / op')

        # Update progress in the same transaction
        progress_tracker.upsert_all_changes_progress(cursor, last_offset, last_timestamp)
    finally:
        cursor.close()

    # Commit both data operations and progress atomically
    db_conn.commit()


def backfill_consumer_process(replay_configs: List[ReplayConfig], opts: argparse.Namespace,
                              stop_events: Dict[str, EventClass], queues: Dict[str, Queue],
                              progress_by_topic: Dict[str, List[Progress]], proc_id: str,
                              logger: logging.Logger) -> None:
    consumer_conf = build_consumer_config(opts.kafka_bootstrap_servers, f'unused-{proc_id}',
                                          **opts.extra_kafka_consumer_config)
    consumer_conf['enable.partition.eof'] = True
    consumer_conf['on_commit'] = commit_cb
    consumer: Consumer = Consumer(consumer_conf)

    try:
        # Build topic partitions for all topics and track high watermarks for EOF detection
        all_topic_partitions: List[TopicPartition] = []
        high_watermarks: Dict[str, Dict[int, int]] = {}  # topic -> partition -> high watermark
        for config in replay_configs:
            topic = config.replay_topic
            progress = progress_by_topic.get(topic, [])
            start_offset_by_partition: Dict[int, int] = {
                p.source_topic_partition: p.last_handled_message_offset + 1 for p in progress
            }
            topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
                topic=topic).topics
            if topics_meta is None:
                raise Exception(f'No partitions found for topic {topic}')
            else:
                partitions = list((topics_meta[topic].partitions or {}).keys())

            # Get high watermarks for each partition to detect EOF
            high_watermarks[topic] = {}
            for p in partitions:
                tp = TopicPartition(topic, p)
                _, high = consumer.get_watermark_offsets(tp)
                high_watermarks[topic][p] = high
                logger.debug(f'Topic {topic} partition {p}: high watermark = {high}')

            topic_partitions: List[TopicPartition] = [TopicPartition(
                topic, p, start_offset_by_partition.get(p, OFFSET_BEGINNING)
            ) for p in partitions]
            all_topic_partitions.extend(topic_partitions)

        logger.debug('Consumer assignments: %s', [f'{tp.topic}:{tp.partition}@{tp.offset}' for tp in all_topic_partitions])
        consumer.assign(all_topic_partitions)
        msg_ctr: int = 0

        msg_ctr_by_topic: Dict[str, int] = {config.replay_topic: 0 for config in replay_configs}

        # Track which topics have been paused (worker hit cutoff)
        paused_topics: Set[str] = set()

        # Track which topic/partitions have reached EOF (for topics with infrequent messages)
        eof_partitions: Dict[str, Set[int]] = {config.replay_topic: set() for config in replay_configs}
        topics_at_eof: Set[str] = set()

        # Map topic names to their partitions for pausing
        topic_to_partitions: Dict[str, List[TopicPartition]] = {}
        for tp in all_topic_partitions:
            if tp.topic not in topic_to_partitions:
                topic_to_partitions[tp.topic] = []
            topic_to_partitions[tp.topic].append(TopicPartition(tp.topic, tp.partition))

        logger.debug(f'Starting shared consumer for topics: {[c.replay_topic for c in replay_configs]}')

        # Check if all workers have stopped
        def all_workers_stopped() -> bool:
            return all(ev.is_set() for ev in stop_events.values())

        while not all_workers_stopped():
            msg = consumer.poll(0.1)

            if msg is None:
                # Check if any workers have stopped and we need to pause their topics
                for topic, event in stop_events.items():
                    if event.is_set() and topic not in paused_topics:
                        logger.info(f'Consumer: worker for topic {topic} has stopped, pausing partition(s)')
                        if topic in topic_to_partitions:
                            consumer.pause(topic_to_partitions[topic])
                            logger.debug(f'Pausing topic {topic} (loc 1)')
                        paused_topics.add(topic)
                continue

            msg_ctr += 1

            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    # Track which partitions have reached EOF
                    eof_topic = msg.topic()
                    eof_partition = msg.partition()
                    assert isinstance(eof_partition, int)
                    if eof_topic and eof_topic not in topics_at_eof:
                        eof_partitions[eof_topic].add(eof_partition)
                        # Check if all partitions for this topic have reached EOF
                        if eof_partitions[eof_topic] == set(high_watermarks[eof_topic].keys()):
                            logger.info(f'Consumer: all partitions for topic {eof_topic} have reached EOF, '
                                       f'sending EOF sentinel to worker')
                            topics_at_eof.add(eof_topic)
                            # Send EOF sentinel to the worker (None values signal EOF)
                            if eof_topic in queues:
                                queues[eof_topic].put((eof_topic, None, None, None, None, None))
                    continue
                else:
                    raise Exception(msg.error())

            raw_topic = msg.topic()
            assert isinstance(raw_topic, str)
            topic = raw_topic

            # Check if this topic's worker has stopped (hit cutoff) - skip and pause if so
            if topic in stop_events and stop_events[topic].is_set():
                if topic not in paused_topics:
                    logger.info(f'Consumer: worker for topic {topic} has stopped, pausing partition(s)')
                    if topic in topic_to_partitions:
                        consumer.pause(topic_to_partitions[topic])
                        logger.debug(f'Pausing topic {topic} (loc 2)')
                    paused_topics.add(topic)
                continue

            if topic in paused_topics:
                continue

            # Route message to the appropriate worker queue
            if topic not in queues:
                logger.warning(f'Received message for unexpected topic: {topic}')
                continue

            msg_ctr_by_topic[topic] += 1

            if msg_ctr_by_topic[topic] % 5_000 == 0:
                logger.debug(f'Topic {topic}: Reached %s, apx queue depth %s',
                            format_coordinates(msg), queues[topic].qsize())

            # Pass raw bytes to workers for deserialization
            raw_key = msg.key()
            raw_val = msg.value()

            retries: int = 0
            while True:
                try:
                    queues[topic].put_nowait((topic, msg.partition(), msg.offset(), msg.timestamp()[1], raw_key, raw_val))
                    break
                except stdlib_queue.Full:
                    if topic in stop_events and stop_events[topic].is_set():
                        logger.warning(f'Queue for topic {topic} full but worker for topic has already signaled '
                                       f'stop. Skipping.')
                        if topic not in paused_topics:
                            consumer.pause(topic_to_partitions[topic])
                            logger.debug(f'Pausing topic {topic} (loc 3)')
                            paused_topics.add(topic)
                        break
                    if retries < 20:
                        retries += 1
                        if retries > 1:
                            logger.warning(f'Retry {retries}. Current apx queue size for topic {topic} '
                                           f'is {queues[topic].qsize()}')
                        time.sleep(2)
                    else:
                        logger.error(f'Persistent queue full attempting to enqueue message for topic {topic}, queue '
                                     f'apx size {queues[topic].qsize()}')
                        raise

            if 0 < opts.consumed_messages_limit <= msg_ctr:
                logger.info(f'Consumed %s messages total, stopping...', opts.consumed_messages_limit)
                break
    finally:
        logger.info("Closing shared consumer...")

        # Set all stop events
        for event in stop_events.values():
            event.set()

        # Close all queues
        for queue in queues.values():
            queue.close()

        consumer.close()

        for queue in queues.values():
            queue.join_thread()

        logger.info("Shared consumer closed.")
        logging.shutdown()
        time.sleep(0.5)
