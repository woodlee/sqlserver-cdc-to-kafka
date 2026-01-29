"""Consumer processes for the replayer module."""

import argparse
import logging
import queue as stdlib_queue
import time
from multiprocessing.synchronize import Event as EventClass
from typing import Dict, List, Optional, Set, Tuple

import ctds  # type: ignore[import-untyped]
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import TopicMetadata
from faster_fifo import Queue  # type: ignore[import-not-found]

from .logging_config import logger as module_logger
from .kafka_utils import build_consumer_config, commit_cb, format_coordinates
from .models import OrderedOperation, Progress, ReplayConfig
from .table_metadata import FollowModeTableMetadata


def _flush_ordered_operations(db_conn: ctds.Connection, ops: List[OrderedOperation],
                              table_metadata: Dict[str, FollowModeTableMetadata]) -> None:
    """Flush operations in strict order, batching only consecutive same-table same-type operations.

    This maintains FK constraint safety by ensuring operations are applied in the order
    they appeared in the all-changes topic. Only consecutive operations targeting the same
    table with the same operation type (delete or upsert) are batched together.
    """
    if not ops:
        return

    start_time = time.perf_counter()

    # Group consecutive operations by (table, operation_type)
    batches: List[Tuple[str, str, List[OrderedOperation]]] = []
    current_topic: Optional[str] = None
    current_op_type: Optional[str] = None
    current_batch: List[OrderedOperation] = []

    for op in ops:
        if op.original_topic == current_topic and op.operation_type == current_op_type:
            current_batch.append(op)
        else:
            if current_batch:
                batches.append((current_topic, current_op_type, current_batch))  # type: ignore[arg-type]
            current_topic = op.original_topic
            current_op_type = op.operation_type
            current_batch = [op]

    if current_batch:
        batches.append((current_topic, current_op_type, current_batch))  # type: ignore[arg-type]

    # Execute batches in order
    with db_conn.cursor() as cursor:
        for topic, op_type, batch in batches:
            metadata = table_metadata[topic]

            if op_type == 'delete':
                # Collect keys and bulk delete
                keys = [op.key_val for op in batch]
                db_conn.bulk_insert(metadata.delete_temp_table_name, keys)
                cursor.execute(metadata.delete_stmt)
                module_logger.debug(f'Follow mode: deleted {len(batch)} rows from {metadata.fq_target_table_name}')
            else:
                # Execute upserts row-by-row to preserve ordering when same PK appears multiple times
                for op in batch:
                    if op.row_values is None:
                        continue
                    if op.cdc_operation == 'Insert':
                        cursor.execute(metadata.insert_stmt, tuple(op.row_values))
                    elif op.cdc_operation == 'PostUpdate':
                        params = metadata.build_update_params(op.row_values)
                        cursor.execute(metadata.update_stmt, params)
                        if cursor.rowcount == 0:
                            raise Exception(f'UPDATE affected 0 rows for {metadata.fq_target_table_name} '
                                           f'with key {op.key_val} - row does not exist')
                    else:
                        raise Exception(f'Unexpected CDC operation type: {op.cdc_operation}')
                module_logger.debug(f'Follow mode: applied {len(batch)} upserts to {metadata.fq_target_table_name}')

    db_conn.commit()
    elapsed_ms = int((time.perf_counter() - start_time) * 1000)
    module_logger.info(f'Follow mode: flushed {len(ops)} operations in {len(batches)} batches ({elapsed_ms} ms)')


def backfill_consumer_process(replay_configs: List[ReplayConfig], opts: argparse.Namespace,
                              stop_events: Dict[str, EventClass], queues: Dict[str, Queue],
                              progress_by_topic: Dict[str, List[Progress]], proc_id: str,
                              logger: logging.Logger) -> None:
    """Shared consumer process that reads from multiple topics and routes messages to workers."""
    consumer_conf = build_consumer_config(opts, f'unused-{proc_id}')
    consumer_conf['enable.partition.eof'] = True
    consumer_conf['on_commit'] = commit_cb
    consumer: Consumer = Consumer(consumer_conf)

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
            topic=topic).topics  # type: ignore[call-arg]
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
        return all(event.is_set() for event in stop_events.values())

    while not all_workers_stopped():
        msg = consumer.poll(0.1)

        if msg is None:
            # Check if any workers have stopped and we need to pause their topics
            for topic, event in stop_events.items():
                if event.is_set() and topic not in paused_topics:
                    logger.info(f'Consumer: worker for topic {topic} has stopped, pausing partition(s)')
                    if topic in topic_to_partitions:
                        consumer.pause(topic_to_partitions[topic])
                    paused_topics.add(topic)
            continue

        msg_ctr += 1

        err = msg.error()
        if err:
            # noinspection PyProtectedMember
            if err.code() == KafkaError._PARTITION_EOF:
                # Track which partitions have reached EOF
                eof_topic = msg.topic()
                eof_partition = msg.partition()
                if eof_topic and eof_topic not in topics_at_eof:
                    eof_partitions[eof_topic].add(eof_partition)  # type: ignore[arg-type]
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

        topic = msg.topic()
        if topic is None:
            raise Exception('Unexpected None value for message topic()')

        # Check if this topic's worker has stopped (hit cutoff) - skip and pause if so
        if topic in stop_events and stop_events[topic].is_set():
            if topic not in paused_topics:
                logger.info(f'Consumer: worker for topic {topic} has stopped, pausing partition(s)')
                if topic in topic_to_partitions:
                    consumer.pause(topic_to_partitions[topic])
                paused_topics.add(topic)
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
                queues[topic].put((topic, msg.partition(), msg.offset(), msg.timestamp()[1], raw_key, raw_val))
                break
            except stdlib_queue.Full:
                if retries < 5:
                    retries += 1
                    time.sleep(3)
                else:
                    raise

        if 0 < opts.consumed_messages_limit <= msg_ctr:
            logger.info(f'Consumed %s messages total, stopping...', opts.consumed_messages_limit)
            break

    # Set all stop events
    for event in stop_events.values():
        event.set()

    # Close all queues
    for queue in queues.values():
        queue.close()

    logger.info("Closing shared consumer.")
    consumer.close()

    for queue in queues.values():
        queue.join_thread()
