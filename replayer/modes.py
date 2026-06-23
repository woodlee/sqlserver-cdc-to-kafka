from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import socket
import sys
import time
from datetime import datetime, timezone, timedelta, UTC
from multiprocessing.synchronize import Event as EventClass
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import TopicMetadata
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faster_fifo import Queue

from .backfill_progress import BackfillProgressTracker, calculate_total_messages_to_process
from .consumer import flush_ordered_operations, backfill_consumer_process
from .kafka_utils import build_consumer_config, get_latest_lsn_from_all_changes_topic, get_lsn_and_command_id_at_offset
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
                topic=topic).topics
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


def run_backfill_mode(opts: argparse.Namespace, replay_configs: List[ReplayConfig],
                      cutoff_override: Optional[Tuple[str, int]] = None,
                      skip_all_changes_progress: bool = False) -> None:
    """Run the replayer in backfill mode - parallel replay up to a cutoff.

    Args:
        cutoff_override: If provided, use this (lsn, command_id) as the cutoff instead of
            reading the latest from the all-changes topic.
        skip_all_changes_progress: If True, don't write all-changes topic progress at the end.
    """
    cutoff_offset: int = -1

    if opts.replay_to:
        cutoff: Tuple[str, int] = opts.replay_to
        logger.info(f"Backfill mode: will replay up to (lsn={cutoff[0]}, command_id={cutoff[1]}) from --replay-to")
    elif cutoff_override:
        cutoff = cutoff_override
        logger.info(f"Backfill mode: will replay up to (lsn={cutoff[0]}, command_id={cutoff[1]}) from cutoff override")
    else:
        cutoff_lsn, cutoff_offset = get_latest_lsn_from_all_changes_topic(opts)
        # Use max command_id so all messages at this LSN are included (matches prior LSN-only behavior)
        cutoff = (cutoff_lsn, 2**31 - 1)
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
        queues[config.replay_topic] = Queue(opts.upsert_batch_size * 3_000)
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

    # Shared error signaling: any worker that dies exceptionally sets this event and puts
    # (topic, traceback_str) on the queue so the main thread can tear everything down fast.
    error_event: EventClass = mp.Event()
    error_queue: mp.Queue = mp.Queue(len(replay_configs))

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
                      worker_proc_id, start_offsets_by_topic.get(config.replay_topic, {}), shared_processed_counter,
                      cutoff, shared_tables_complete_counter, error_event, error_queue)
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

            # Check if any worker died exceptionally
            if error_event.is_set():
                failed_topic = '(unknown)'
                tb_str = '(no traceback available)'
                try:
                    failed_topic, tb_str = error_queue.get_nowait()
                except Exception:
                    pass
                logger.error(f"Worker for topic '{failed_topic}' died with exception. "
                             f"Initiating full shutdown for safe restart.\n{tb_str}")
                for event in stop_events.values():
                    event.set()
                time.sleep(1)
                for w in workers:
                    if w.is_alive():
                        w.terminate()
                if consumer_proc.is_alive():
                    consumer_proc.terminate()
                for w in workers:
                    w.join(timeout=5)
                consumer_proc.join(timeout=5)
                sys.exit(1)

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
        logger.info("All replay workers have exited.")
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down workers...")
        for event in stop_events.values():
            event.set()
        time.sleep(1)
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
        time.sleep(1)
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
        if consumer_proc.is_alive():
            consumer_proc.terminate()
        for worker in workers:
            worker.join(timeout=5)
        consumer_proc.join(timeout=5)
    finally:
        if not skip_all_changes_progress and cutoff_offset >= 0 and not error_event.is_set():
            progress_tracker.commit_all_changes_topic_progress(cutoff_offset, datetime.now())
            logger.info(f"Backfill complete. Wrote all-changes topic progress at offset {cutoff_offset} "
                        f"for follow mode handoff.")
        else:
            logger.info(f"Backfill complete. ** FINAL ALL-CHANGES TOPIC PROGRESS WAS NOT WRITTEN!! ** "
                        f"(cutoff_offset value was '{cutoff_offset}')")

        time.sleep(0.5)
        logging.shutdown()
        time.sleep(0.5)


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
    schema_registry_client = SchemaRegistryClient({'url': opts.schema_registry_url, 'timeout': 15.0})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
        topic=opts.all_changes_topic).topics
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
    last_all_changes_timestamp: datetime = datetime(1900, 1, 1)
    last_commit_time = datetime.now()
    last_heartbeat_time = datetime.now()

    try:
        while True:
            if datetime.now() >= last_heartbeat_time + timedelta(seconds=30):
                lag_seconds = int((last_commit_time - last_all_changes_timestamp).total_seconds())
                logger.info(f"Follow mode: heartbeat: last_all_changes_offset {last_all_changes_offset} "
                            f"last_all_changes_timestamp {last_all_changes_timestamp} last_commit_time "
                            f"{last_commit_time} msg_ctr {msg_ctr} lag {lag_seconds} seconds")
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
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise Exception(msg.error())

            msg_ctr += 1

            raw_offset = msg.offset()
            if raw_offset is None:
                raise Exception(msg.error())
            offset: int = raw_offset

            # Get the original topic from message header
            headers = msg.headers() or []
            original_topic = None
            for header_key, header_val in headers:
                if header_key == 'cdc_to_kafka_original_topic':
                    original_topic = header_val.decode('utf-8') if isinstance(header_val, bytes) else header_val
                    break

            msg_timestamp = datetime.fromtimestamp(msg.timestamp()[1] / 1000, timezone.utc).replace(tzinfo=None)

            if original_topic is None:
                logger.error(f'Message at offset {offset} missing cdc_to_kafka_original_topic header, skipping')
                last_all_changes_offset = offset
                last_all_changes_timestamp = msg_timestamp
                continue

            if original_topic not in table_metadata:
                # This table isn't in our replay config, skip it
                last_all_changes_offset = offset
                last_all_changes_timestamp = msg_timestamp
                continue

            # Deserialize the message
            raw_key = msg.key()
            raw_val = msg.value()

            if raw_key is None:
                msg_key = None
            else:
                msg_key = avro_deserializer(raw_key, SerializationContext(original_topic, MessageField.KEY))

            if raw_val is None:
                msg_val = None
            else:
                msg_val = avro_deserializer(raw_val, SerializationContext(original_topic, MessageField.VALUE))

            # Prepare the operation (without executing)
            metadata = table_metadata[original_topic]

            assert isinstance(msg_key, dict)
            assert isinstance(msg_val, dict)

            # Check replay_to cutoff
            if opts.replay_to is not None:
                msg_lsn = msg_val.get('__log_lsn', '')
                msg_command_id = msg_val.get('__command_id', 0)
                if msg_lsn and (msg_lsn, msg_command_id) > opts.replay_to:
                    logger.info(f"Follow mode: reached replay_to cutoff at offset {offset} "
                               f"(msg ({msg_lsn}, {msg_command_id}) > {opts.replay_to}). Flushing and stopping.")
                    if ordered_ops:
                        flush_ordered_operations(db_conn, progress_tracker, ordered_ops, table_metadata,
                                                 last_all_changes_offset, last_all_changes_timestamp)
                        ordered_ops.clear()
                    break

            if opts.minimum_lag_seconds:
                lag_time = opts.minimum_lag_seconds - (datetime.now(UTC).replace(tzinfo=None) - msg_timestamp).total_seconds()
                if lag_time > 0:
                    time.sleep(lag_time)

            op = metadata.prepare_operation(msg_key, msg_val, offset, msg_timestamp)
            if op is not None:
                ordered_ops.append(op)

            last_all_changes_offset = offset
            last_all_changes_timestamp = msg_timestamp

            if msg_ctr % 5_000 == 0:
                logger.debug(f'Follow mode: processed {msg_ctr} messages, at offset {last_all_changes_offset}, '
                            f'pending ops: {len(ordered_ops)}')

            # TODO if needed for perf: separate consuming and Avro deser into a separate process that hands off
            # deser'd messages to this existing flow via a faster_fifo Queue.

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


def _discover_foreign_keys(db_conn: pyodbc.Connection,
                           replay_configs: List[ReplayConfig]) -> List[Dict[str, Any]]:
    """Find all FK constraints that reference or are referenced by the target tables.

    Returns a list of dicts with keys: constraint_name, child_schema, child_table,
    parent_schema, parent_table, child_columns, parent_columns,
    delete_action, update_action.
    """
    table_fq_names = [f'[{c.target_db_table_schema.strip()}].[{c.target_db_table_name.strip()}]'
                      for c in replay_configs]

    # Build OBJECT_ID list for the WHERE clause
    object_id_checks = ' OR '.join(
        [f"fk.parent_object_id = OBJECT_ID(?) OR fk.referenced_object_id = OBJECT_ID(?)"
         for _ in table_fq_names]
    )
    params: List[str] = []
    for fq in table_fq_names:
        params.extend([fq, fq])

    cursor = db_conn.cursor()
    try:
        # Get FK metadata
        cursor.execute(f'''
SELECT
    fk.object_id AS fk_id,
    fk.name AS constraint_name,
    SCHEMA_NAME(child_t.schema_id) AS child_schema,
    child_t.name AS child_table,
    SCHEMA_NAME(parent_t.schema_id) AS parent_schema,
    parent_t.name AS parent_table,
    fk.delete_referential_action_desc AS delete_action,
    fk.update_referential_action_desc AS update_action
FROM sys.foreign_keys fk
JOIN sys.tables child_t ON fk.parent_object_id = child_t.object_id
JOIN sys.tables parent_t ON fk.referenced_object_id = parent_t.object_id
WHERE {object_id_checks}
        ''', params)

        fk_rows = cursor.fetchall()
        fk_infos: List[Dict[str, Any]] = []

        for fk_id, constraint_name, child_schema, child_table, parent_schema, parent_table, \
                delete_action, update_action in fk_rows:
            # Get column mappings for this FK
            cursor.execute('''
SELECT
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS child_col,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS parent_col
FROM sys.foreign_key_columns fkc
WHERE fkc.constraint_object_id = ?
ORDER BY fkc.constraint_column_id
            ''', (fk_id,))
            col_rows = cursor.fetchall()
            child_columns = [r[0] for r in col_rows]
            parent_columns = [r[1] for r in col_rows]

            fk_infos.append({
                'constraint_name': constraint_name,
                'child_schema': child_schema,
                'child_table': child_table,
                'parent_schema': parent_schema,
                'parent_table': parent_table,
                'child_columns': child_columns,
                'parent_columns': parent_columns,
                'delete_action': delete_action,
                'update_action': update_action,
            })

        return fk_infos
    finally:
        cursor.close()


def _generate_fk_restore_commands(fk_infos: List[Dict[str, Any]]) -> List[str]:
    """Generate ALTER TABLE ... ADD CONSTRAINT statements to recreate dropped FK constraints."""
    commands: List[str] = []
    for fk in fk_infos:
        child_cols = ', '.join(f'[{c}]' for c in fk['child_columns'])
        parent_cols = ', '.join(f'[{c}]' for c in fk['parent_columns'])

        action_clauses = ''
        if fk['delete_action'] != 'NO_ACTION':
            action_clauses += f" ON DELETE {fk['delete_action'].replace('_', ' ')}"
        if fk['update_action'] != 'NO_ACTION':
            action_clauses += f" ON UPDATE {fk['update_action'].replace('_', ' ')}"

        cmd = (f"ALTER TABLE [{fk['child_schema']}].[{fk['child_table']}] "
               f"WITH CHECK ADD CONSTRAINT [{fk['constraint_name']}] "
               f"FOREIGN KEY ({child_cols}) "
               f"REFERENCES [{fk['parent_schema']}].[{fk['parent_table']}] ({parent_cols})"
               f"{action_clauses};")
        commands.append(cmd)
    return commands


def _drop_foreign_keys(db_conn: pyodbc.Connection, fk_infos: List[Dict[str, Any]]) -> None:
    """Drop all specified FK constraints."""
    cursor = db_conn.cursor()
    try:
        for fk in fk_infos:
            drop_stmt = (f"ALTER TABLE [{fk['child_schema']}].[{fk['child_table']}] "
                         f"DROP CONSTRAINT [{fk['constraint_name']}];")
            logger.info(f"Dropping FK: {drop_stmt}")
            cursor.execute(drop_stmt)
        db_conn.commit()
    finally:
        cursor.close()


def _safe_drop_foreign_keys(db_conn: pyodbc.Connection, replay_configs: List[ReplayConfig]) -> None:
    """Discover and drop any FK constraints involving the target tables.

    Safe to call even if FKs have already been dropped (e.g. during redo_continue after an
    interrupted redo_from_beginning). Logs restore commands before dropping.
    """
    fk_infos = _discover_foreign_keys(db_conn, replay_configs)
    if fk_infos:
        logger.info(f"Found {len(fk_infos)} FK constraint(s) involving target tables")

        restore_commands = _generate_fk_restore_commands(fk_infos)
        logger.info("=" * 80)
        logger.info("FK RESTORE COMMANDS - save these for manual execution after redo completes:")
        logger.info("=" * 80)
        for cmd in restore_commands:
            logger.info(cmd)
        logger.info("=" * 80)

        _drop_foreign_keys(db_conn, fk_infos)
        logger.info("All FK constraints dropped successfully")
    else:
        logger.info("No FK constraints found involving target tables")


def run_redo_continue_mode(opts: argparse.Namespace, replay_configs: List[ReplayConfig]) -> None:
    """Run the replayer in redo_continue mode - resume a previously interrupted redo.

    This mode is safe to run repeatedly. It will:
      1. Determine the cutoff (from --replay-to or from latest LSN on the all-changes topic)
      2. Drop any remaining FK constraints (safe no-op if already dropped)
      3. Replay from existing progress (or beginning if progress was already cleared)

    NOTE: If a prior redo_from_beginning was interrupted during the truncation phase (after
    some tables were truncated but before progress was cleared for all of them), the data
    state may be inconsistent. In that case, run redo_from_beginning again instead.
    """
    # 1. Determine cutoff
    if opts.replay_to:
        cutoff = opts.replay_to
        logger.info(f"Redo continue: using --replay-to cutoff (lsn={cutoff[0]}, command_id={cutoff[1]})")
    else:
        cutoff_lsn, _ = get_latest_lsn_from_all_changes_topic(opts)
        cutoff = (cutoff_lsn, 2**31 - 1)
        logger.info(f"Redo continue: using latest all-changes topic LSN as cutoff: {cutoff_lsn}")

    # 2. Safe FK re-check (no-op if already dropped)
    pyodbc_conn_str = get_pyodbc_conn_string_from_opts(opts)
    db_conn = pyodbc.connect(pyodbc_conn_str, autocommit=True)
    try:
        _safe_drop_foreign_keys(db_conn, replay_configs)
    finally:
        db_conn.close()

    # 3. Replay from existing progress with cutoff
    logger.info("Starting replay phase...")
    run_backfill_mode(opts, replay_configs, cutoff_override=cutoff, skip_all_changes_progress=True)


def run_redo_from_beginning_mode(opts: argparse.Namespace, replay_configs: List[ReplayConfig]) -> None:
    """Run the replayer in redo_from_beginning mode - drop FKs, truncate, and replay from scratch.

    Steps:
      1. Determine the cutoff (from --replay-to or from all-changes topic progress)
      2. Discover and drop FK constraints referencing or from the target tables
      3. Print commands to restore those FKs for later manual execution
      4. Truncate the target tables
      5. Clear replay progress for those tables
      6. Replay from the beginning of each topic up to the cutoff

    If this mode is interrupted after step 4/5 but before replay completes, use
    redo_continue to resume without re-truncating.
    """
    proc_id: str = f'{socket.getfqdn()}+{int(datetime.now().timestamp())}'
    pyodbc_conn_str = get_pyodbc_conn_string_from_opts(opts)

    progress_tracker = ProgressTracker(pyodbc_conn_str, opts.progress_tracking_table_schema,
                                       opts.progress_tracking_table_name, opts.all_changes_topic,
                                       opts.progress_tracking_namespace, proc_id)

    # 1. Determine cutoff
    if opts.replay_to:
        cutoff = opts.replay_to
        logger.info(f"Redo from beginning: using --replay-to cutoff (lsn={cutoff[0]}, command_id={cutoff[1]})")
    else:
        all_changes_progress = progress_tracker.get_all_changes_topic_progress()
        if all_changes_progress is None:
            raise Exception(f'No --replay-to specified and no all-changes topic progress found for '
                           f'"{opts.all_changes_topic}" in namespace "{opts.progress_tracking_namespace}". '
                           f'Either provide --replay-to or run backfill mode first.')
        cutoff = get_lsn_and_command_id_at_offset(opts, all_changes_progress.last_handled_message_offset)
        logger.info(f"Redo from beginning: cutoff from all-changes progress at offset "
                    f"{all_changes_progress.last_handled_message_offset}: "
                    f"(lsn={cutoff[0]}, command_id={cutoff[1]})")

    # 2-3. Discover, log restore commands, and drop FK constraints
    db_conn = pyodbc.connect(pyodbc_conn_str, autocommit=True)
    try:
        _safe_drop_foreign_keys(db_conn, replay_configs)

        # 4. Truncate target tables
        cursor = db_conn.cursor()
        try:
            for config in replay_configs:
                fq_name = f'[{config.target_db_table_schema.strip()}].[{config.target_db_table_name.strip()}]'
                logger.info(f"Truncating table {fq_name}")
                cursor.execute(f'TRUNCATE TABLE {fq_name};')
            db_conn.commit()
        finally:
            cursor.close()

        # 5. Clear replay progress for these tables
        for config in replay_configs:
            progress_tracker.delete_topic_progress(
                config.target_db_table_schema, config.target_db_table_name, config.replay_topic)
        logger.info("Cleared all replay progress for target tables")
    finally:
        db_conn.close()

    # 6. Run the replay (reuse backfill machinery with our cutoff, skip writing all-changes progress)
    logger.info("Starting replay phase...")
    run_backfill_mode(opts, replay_configs, cutoff_override=cutoff, skip_all_changes_progress=True)
