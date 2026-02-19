from __future__ import annotations

import argparse
import multiprocessing as mp
import socket
import time
from datetime import datetime, timezone, timedelta
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event as EventClass
from typing import Any, Dict, List, Optional

import pyodbc
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import TopicMetadata
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faster_fifo import Queue  # type: ignore[import-not-found]

from .backfill_progress import BackfillProgressTracker, calculate_total_messages_to_process
from .consumer import flush_ordered_operations, backfill_consumer_process
from .kafka_utils import build_consumer_config, get_latest_lsn_from_all_changes_topic
from .logging_config import get_logger
from .models import OrderedOperation, Progress, ReplayConfig
from .progress import ProgressTracker
from .table_metadata import FollowModeTableMetadata
from .utils import get_pyodbc_conn_string_from_opts
from .worker import replay_worker

logger = get_logger(__name__)


def _get_high_watermarks_for_topics(
    opts: argparse.Namespace,
    replay_configs: List[ReplayConfig]
) -> Dict[str, Dict[int, int]]:
    """Get high watermarks for all topic partitions.

    Returns a dict of topic -> partition -> high watermark offset.
    """
    consumer_conf = build_consumer_config(opts.kafka_bootstrap_servers, 'watermark-check',
                                          **opts.extra_kafka_consumer_config)
    consumer = Consumer(consumer_conf)
    high_watermarks: Dict[str, Dict[int, int]] = {}

    try:
        for config in replay_configs:
            topic = config.replay_topic
            topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
                topic=topic).topics  # type: ignore[call-arg]
            if topics_meta is None:
                raise Exception(f'No partitions found for topic {topic}')

            partitions = list((topics_meta[topic].partitions or {}).keys())
            high_watermarks[topic] = {}

            for p in partitions:
                tp = TopicPartition(topic, p)
                _, high = consumer.get_watermark_offsets(tp)
                high_watermarks[topic][p] = high
    finally:
        consumer.close()

    return high_watermarks


def run_backfill_mode(opts: argparse.Namespace, replay_configs: List[ReplayConfig]) -> None:
    """Run the replayer in backfill mode - parallel replay up to a cutoff LSN."""
    # Get cutoff LSN from the all-changes topic
    cutoff_lsn, cutoff_offset = get_latest_lsn_from_all_changes_topic(opts)
    logger.info(f"Backfill mode: will replay up to LSN {cutoff_lsn}")

    # Create shared structures for the consumer process
    stop_events: Dict[str, EventClass] = {}
    queues: Dict[str, Queue] = {}
    progress_by_topic: Dict[str, List[Progress]] = {}
    proc_id: str = f'{socket.getfqdn()}+{int(datetime.now().timestamp())}'

    pyodbc_conn_str = get_pyodbc_conn_string_from_opts(opts)
    progress_tracker = ProgressTracker(pyodbc_conn_str, opts.progress_tracking_table_schema,
                                       opts.progress_tracking_table_name, opts.all_changes_topic,
                                       opts.progress_tracking_namespace, proc_id)

    # Get progress for each topic
    for config in replay_configs:
        stop_events[config.replay_topic] = mp.Event()
        # For faster_fifo the ctor arg here is the queue byte size, not its item count size:
        queues[config.replay_topic] = Queue(opts.upsert_batch_size * 10_000)
        worker_opts = argparse.Namespace(**vars(opts))
        worker_opts.replay_topic = config.replay_topic
        worker_opts.target_db_table_schema = config.target_db_table_schema
        worker_opts.target_db_table_name = config.target_db_table_name
        progress_by_topic[config.replay_topic] = progress_tracker.get_progress(
            worker_opts.target_db_table_schema, worker_opts.target_db_table_name, worker_opts.replay_topic)

    # Calculate total messages to process and set up progress tracking
    high_watermarks = _get_high_watermarks_for_topics(opts, replay_configs)
    total_messages = calculate_total_messages_to_process(high_watermarks, progress_by_topic)
    backfill_progress = BackfillProgressTracker()
    backfill_progress.set_total_to_process(total_messages)
    backfill_progress.set_total_tables(len(replay_configs))
    shared_processed_counter = backfill_progress.get_shared_counter()
    shared_tables_complete_counter = backfill_progress.get_tables_complete_counter()

    # Compute per-topic start offsets so workers can track progress via offset deltas
    start_offsets_by_topic: Dict[str, Dict[int, int]] = {}
    for topic, partition_watermarks in high_watermarks.items():
        progress_records = progress_by_topic.get(topic, [])
        last_offset_by_partition: Dict[int, int] = {
            p.source_topic_partition: p.last_handled_message_offset for p in progress_records
        }
        start_offsets_by_topic[topic] = {
            partition: last_offset_by_partition.get(partition, -1) + 1
            for partition in partition_watermarks
        }

    # Launch single shared consumer process (with cutoff LSN for backfill mode)
    consumer_proc = mp.Process(
        target=backfill_consumer_process,
        name='shared-consumer',
        args=(replay_configs, opts, stop_events, queues, progress_by_topic, proc_id, logger)
    )

    # Launch worker processes for each topic/table pair
    workers: List[mp.Process] = []

    try:
        consumer_proc.start()
        logger.info("Launched shared consumer process")

        for config in replay_configs:
            time.sleep(0.2)
            worker_proc_id = f'{proc_id}+{config.replay_topic}'
            worker = mp.Process(
                target=replay_worker,
                name=f'replayer-{config.replay_topic}',
                args=(config, opts, stop_events[config.replay_topic], queues[config.replay_topic],
                      worker_proc_id, cutoff_lsn, shared_processed_counter,
                      start_offsets_by_topic.get(config.replay_topic, {}),
                      shared_tables_complete_counter)
            )
            worker.start()
            workers.append(worker)
            logger.debug(f"Launched worker process for '{config.replay_topic}' -> "
                        f"[{config.target_db_table_schema}].[{config.target_db_table_name}]")

        # Wait for all workers to complete while displaying progress
        last_progress_log_time = datetime.now()
        last_queue_depth_log_time = datetime.now()
        progress_log_interval = timedelta(seconds=15)
        queue_depth_log_interval = timedelta(seconds=60)

        while any(w.is_alive() for w in workers) or consumer_proc.is_alive():
            now = datetime.now()
            # Check if it's time to log progress
            if now >= last_progress_log_time + progress_log_interval:
                logger.info(backfill_progress.format_progress_report())
                last_progress_log_time = now

            # Less-frequent queue depth report for diagnosing hangs
            if now >= last_queue_depth_log_time + queue_depth_log_interval:
                depth_parts = []
                for config, worker in zip(replay_configs, workers):
                    if worker.is_alive():
                        q = queues[config.replay_topic]
                        depth_parts.append(f"{config.replay_topic}={q.qsize()}")
                if depth_parts:
                    logger.info(f"Queue depths for active workers: {', '.join(depth_parts)}")
                last_queue_depth_log_time = now

            time.sleep(1)

        # Final progress report
        logger.info(f"Final: {backfill_progress.format_progress_report()}")
        logger.info("All replay workers have completed.")

        progress_tracker.commit_all_changes_topic_progress(cutoff_offset, datetime.now())
        logger.info(f"Backfill complete. Wrote all-changes topic progress at offset {cutoff_offset} "
                   f"for follow mode handoff.")

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down workers...")
        for event in stop_events.values():
            event.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
        if consumer_proc.is_alive():
            consumer_proc.terminate()
        for worker in workers:
            worker.join(timeout=5)
        consumer_proc.join(timeout=5)
    except Exception as e:
        logger.exception(f"Error in main process: {e}")
        for event in stop_events.values():
            event.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
        if consumer_proc.is_alive():
            consumer_proc.terminate()
        for worker in workers:
            worker.join(timeout=5)
        consumer_proc.join(timeout=5)


def run_follow_mode(opts: argparse.Namespace, replay_configs: List[ReplayConfig]) -> None:
    """Run the replayer in follow mode - strictly ordered replay from the all-changes topic.

    This mode processes messages from the all-changes topic in strict LSN order, applying
    each operation immediately to maintain FK constraint safety. Operations are batched
    and committed atomically with progress updates to ensure exactly-once semantics.

    Uses pyodbc for all database operations to avoid the ctds/FreeTDS issue where empty
    strings are converted to NULL. Data operations and progress updates are committed
    together in a single transaction for crash safety.
    """
    proc_id: str = f'{socket.getfqdn()}+{int(datetime.now().timestamp())}'

    pyodbc_conn_str = (f'DRIVER={{ODBC Driver 18 for SQL Server}};'
                       f'SERVER={opts.target_db_server};'
                       f'DATABASE={opts.target_db_database};'
                       f'UID={opts.target_db_user};'
                       f'PWD={opts.target_db_password};'
                       f'TrustServerCertificate=yes;')

    progress_tracker = ProgressTracker(pyodbc_conn_str, opts.progress_tracking_table_schema,
                                       opts.progress_tracking_table_name, opts.all_changes_topic,
                                       opts.progress_tracking_namespace, proc_id)

    db_conn = pyodbc.connect(pyodbc_conn_str, autocommit=False)

    all_changes_progress = progress_tracker.get_all_changes_topic_progress()
    if all_changes_progress is None:
        raise Exception(f'No progress found for all-changes topic "{opts.all_changes_topic}" in namespace '
                       f'"{opts.progress_tracking_namespace}". Run backfill mode first to establish progress.')

    start_offset = all_changes_progress.last_handled_message_offset + 1
    logger.info(f"Follow mode: starting from all-changes topic offset {start_offset}")

    table_metadata: Dict[str, FollowModeTableMetadata] = {}
    for config in replay_configs:
        metadata = FollowModeTableMetadata(config, db_conn)
        table_metadata[config.replay_topic] = metadata

    consumer_conf = build_consumer_config(opts.kafka_bootstrap_servers, f'replayer-follow-{proc_id}',
                                          **opts.extra_kafka_consumer_config)
    consumer = Consumer(consumer_conf)
    schema_registry_client = SchemaRegistryClient({'url': opts.schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
        topic=opts.all_changes_topic).topics  # type: ignore[call-arg]
    if topics_meta is None:
        raise Exception(f'No metadata found for topic {opts.all_changes_topic}')

    partitions = list((topics_meta[opts.all_changes_topic].partitions or {}).keys())
    if len(partitions) != 1:
        logger.warning(f'All-changes topic has {len(partitions)} partitions; expected 1 for ordered processing')

    topic_partitions = [TopicPartition(opts.all_changes_topic, p, start_offset) for p in partitions]
    consumer.assign(topic_partitions)

    logger.info(f"Follow mode: consuming from {opts.all_changes_topic}, assigned partitions: {partitions}")

    # Ordered queue of operations to apply
    ordered_ops: List[OrderedOperation] = []
    msg_ctr = 0
    last_all_changes_offset = start_offset - 1
    last_all_changes_timestamp = 0
    last_commit_time = datetime.now()
    last_heartbeat_time = datetime.now()

    try:
        while True:
            if datetime.now() >= last_heartbeat_time + timedelta(seconds=30):
                logger.info(f"Follow mode: heartbeat: last_all_changes_offset {last_all_changes_offset} "
                            f"last_all_changes_timestamp {last_all_changes_timestamp} last_commit_time "
                            f"{last_commit_time} msg_ctr {msg_ctr}")
                last_heartbeat_time = datetime.now()

            msg = consumer.poll(0.5)

            if msg is None or msg.value() is None:
                # Periodically flush and commit progress even without new messages
                if (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds:
                    if ordered_ops or last_all_changes_offset >= start_offset:
                        # Atomically commit data ops (if any) + progress update
                        flush_ordered_operations(db_conn, progress_tracker, ordered_ops, table_metadata,
                                                 last_all_changes_offset, last_all_changes_timestamp)
                        ordered_ops.clear()
                    last_commit_time = datetime.now()
                continue

            err = msg.error()
            if err:
                # noinspection PyProtectedMember
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise Exception(msg.error())

            msg_ctr += 1

            # Get the original topic from message header
            headers = msg.headers() or []
            original_topic = None
            for header_key, header_val in headers:
                if header_key == 'cdc_to_kafka_original_topic':
                    original_topic = header_val.decode('utf-8') if isinstance(header_val, bytes) else header_val
                    break

            if original_topic is None:
                logger.error(f'Message at offset {msg.offset()} missing cdc_to_kafka_original_topic header, skipping')
                last_all_changes_offset = msg.offset()  # type: ignore[assignment]
                last_all_changes_timestamp = msg.timestamp()[1]  # type: ignore[assignment]
                continue

            if original_topic not in table_metadata:
                # This table isn't in our replay config, skip it
                last_all_changes_offset = msg.offset()  # type: ignore[assignment]
                last_all_changes_timestamp = msg.timestamp()[1]  # type: ignore[assignment]
                continue

            # Deserialize the message
            raw_key = msg.key()
            raw_val = msg.value()

            msg_key: Optional[Dict[str, Any]] = None
            msg_val: Optional[Dict[str, Any]] = None
            if raw_key is not None:
                # noinspection PyNoneFunctionAssignment
                msg_key = avro_deserializer(raw_key, SerializationContext(original_topic, MessageField.KEY))  # type: ignore[func-returns-value]
            if raw_val is not None:
                # noinspection PyNoneFunctionAssignment
                msg_val = avro_deserializer(raw_val, SerializationContext(original_topic, MessageField.VALUE))  # type: ignore[func-returns-value]

            # Prepare the operation (without executing)
            metadata = table_metadata[original_topic]
            msg_timestamp = datetime.fromtimestamp(msg.timestamp()[1] / 1000, timezone.utc).replace(tzinfo=None)
            op = metadata.prepare_operation(msg_key, msg_val, msg.offset(), msg_timestamp)
            if op is not None:
                ordered_ops.append(op)

            last_all_changes_offset = msg.offset()  # type: ignore[assignment]
            last_all_changes_timestamp = msg_timestamp

            if msg_ctr % 5_000 == 0:
                logger.debug(f'Follow mode: processed {msg_ctr} messages, at offset {last_all_changes_offset}, '
                            f'pending ops: {len(ordered_ops)}')

            # Periodically flush and commit (but maintain order within each flush)
            # Atomically commits data operations + progress update in a single transaction
            if (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds or \
                    len(ordered_ops) >= opts.upsert_batch_size:
                flush_ordered_operations(db_conn, progress_tracker, ordered_ops, table_metadata,
                                         last_all_changes_offset, msg_timestamp)
                ordered_ops.clear()
                last_commit_time = datetime.now()

            if 0 < opts.consumed_messages_limit <= msg_ctr:
                logger.info(f'Consumed {msg_ctr} messages, stopping...')
                break

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down follow mode...")
    finally:
        for metadata in table_metadata.values():
            metadata.log_stats()
        consumer.close()
        db_conn.close()
        logger.info(f"Follow mode: processed {msg_ctr} messages total, final offset {last_all_changes_offset}")
