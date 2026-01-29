"""Execution modes for the replayer - backfill and follow."""

import argparse
import multiprocessing as mp
import socket
import time
from datetime import datetime
from multiprocessing.synchronize import Event as EventClass
from typing import Any, Dict, List, Optional

import ctds  # type: ignore[import-untyped]
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import TopicMetadata
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faster_fifo import Queue  # type: ignore[import-not-found]

from .consumer import _flush_ordered_operations, backfill_consumer_process
from .kafka_utils import build_consumer_config, get_latest_lsn_from_all_changes_topic
from .logging_config import logger
from .models import OrderedOperation, Progress, ReplayConfig
from .progress import commit_all_changes_topic_progress, get_all_changes_topic_progress, get_progress
from .table_metadata import FollowModeTableMetadata
from .worker import replay_worker


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

    # Get progress for each topic
    for config in replay_configs:
        stop_events[config.replay_topic] = mp.Event()
        # For faster_fifo the ctor arg here is the queue byte size, not its item count size:
        queues[config.replay_topic] = Queue(opts.upsert_batch_size * 5_000)

        # Need to fetch progress for each topic
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                         database=opts.target_db_database, timeout=30, login_timeout=30) as db_conn:
            worker_opts = argparse.Namespace(**vars(opts))
            worker_opts.replay_topic = config.replay_topic
            worker_opts.target_db_table_schema = config.target_db_table_schema
            worker_opts.target_db_table_name = config.target_db_table_name
            progress_by_topic[config.replay_topic] = get_progress(worker_opts, db_conn)

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
                      worker_proc_id, cutoff_lsn)
            )
            worker.start()
            workers.append(worker)
            logger.debug(f"Launched worker process for '{config.replay_topic}' -> "
                        f"[{config.target_db_table_schema}].[{config.target_db_table_name}]")

        # Wait for all workers to complete
        for worker in workers:
            worker.join()

        # Wait for consumer to finish
        consumer_proc.join()
        logger.info("All replay workers have completed.")

        # Write progress for the all-changes topic so follow mode knows where to start
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                         database=opts.target_db_database, timeout=30, login_timeout=30) as db_conn:
            # Use current timestamp for the progress record
            commit_all_changes_topic_progress(opts, cutoff_offset, int(datetime.now().timestamp() * 1000),
                                              proc_id, db_conn)
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
    each operation immediately to maintain FK constraint safety. Operations are executed
    one at a time (or in small batches of consecutive same-table same-type operations)
    to ensure parent rows exist before child rows are inserted.
    """
    proc_id: str = f'{socket.getfqdn()}+{int(datetime.now().timestamp())}'

    # Use a single shared database connection for all operations
    db_conn = ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                           database=opts.target_db_database, timeout=30, login_timeout=30)

    all_changes_progress = get_all_changes_topic_progress(opts, db_conn)
    if all_changes_progress is None:
        raise Exception(f'No progress found for all-changes topic "{opts.all_changes_topic}" in namespace '
                       f'"{opts.progress_tracking_namespace}". Run backfill mode first to establish progress.')

    start_offset = all_changes_progress.last_handled_message_offset + 1
    logger.info(f"Follow mode: starting from all-changes topic offset {start_offset}")

    table_metadata: Dict[str, FollowModeTableMetadata] = {}
    for config in replay_configs:
        metadata = FollowModeTableMetadata(config, db_conn)
        metadata.create_temp_tables(db_conn)
        table_metadata[config.replay_topic] = metadata

    consumer_conf = build_consumer_config(opts, f'replayer-follow-{proc_id}')
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

    try:
        while True:
            msg = consumer.poll(0.5)

            if msg is None or msg.value() is None:
                # Periodically flush and commit progress even without new messages
                if (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds:
                    if ordered_ops:
                        _flush_ordered_operations(db_conn, ordered_ops, table_metadata)
                        ordered_ops.clear()
                    if last_all_changes_offset >= start_offset:
                        commit_all_changes_topic_progress(opts, last_all_changes_offset,
                                                          last_all_changes_timestamp, proc_id, db_conn)
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
                msg_key = avro_deserializer(raw_key, SerializationContext(original_topic, MessageField.KEY))  # type: ignore[func-returns-value]
            if raw_val is not None:
                msg_val = avro_deserializer(raw_val, SerializationContext(original_topic, MessageField.VALUE))  # type: ignore[func-returns-value]

            # Prepare the operation (without executing)
            metadata = table_metadata[original_topic]
            op = metadata.prepare_operation(msg_key, msg_val)
            if op is not None:
                ordered_ops.append(op)

            last_all_changes_offset = msg.offset()  # type: ignore[assignment]
            last_all_changes_timestamp = msg.timestamp()[1]  # type: ignore[assignment]

            if msg_ctr % 5_000 == 0:
                logger.debug(f'Follow mode: processed {msg_ctr} messages, at offset {last_all_changes_offset}, '
                            f'pending ops: {len(ordered_ops)}')

            # Periodically flush and commit (but maintain order within each flush)
            if (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds or \
                    len(ordered_ops) >= opts.upsert_batch_size:
                if ordered_ops:
                    _flush_ordered_operations(db_conn, ordered_ops, table_metadata)
                    ordered_ops.clear()
                commit_all_changes_topic_progress(opts, last_all_changes_offset,
                                                  last_all_changes_timestamp, proc_id, db_conn)
                last_commit_time = datetime.now()

            if 0 < opts.consumed_messages_limit <= msg_ctr:
                logger.info(f'Consumed {msg_ctr} messages, stopping...')
                break

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down follow mode...")
    finally:
        # Final flush
        if ordered_ops:
            _flush_ordered_operations(db_conn, ordered_ops, table_metadata)
        if last_all_changes_offset >= start_offset:
            commit_all_changes_topic_progress(opts, last_all_changes_offset,
                                              last_all_changes_timestamp, proc_id, db_conn)

        # Log stats for each table
        for metadata in table_metadata.values():
            metadata.log_stats()

        consumer.close()
        db_conn.close()
        logger.info(f"Follow mode: processed {msg_ctr} messages total, final offset {last_all_changes_offset}")
