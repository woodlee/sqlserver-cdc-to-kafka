import argparse
import pprint
import queue as stdlib_queue
import time
from datetime import datetime
from multiprocessing.synchronize import Event as EventClass
from typing import Any, Dict, List, Optional, Set, Tuple

import _tds  # type: ignore[import-not-found]
import ctds  # type: ignore[import-untyped]
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faster_fifo import Queue  # type: ignore[import-not-found]

from .logging_config import get_logger
from .models import ReplayConfig
from .progress import ProgressTracker
from .table_metadata import TableMetadata
from .utils import get_pyodbc_conn_string_from_opts

logger = get_logger(__name__)


def replay_worker(config: ReplayConfig, opts: argparse.Namespace, stop_event: EventClass, queue: Queue,
                  proc_id: str, cutoff_lsn: str = None) -> None:
    """Worker process that replays a single topic to a single table.

    If cutoff_lsn is provided (backfill mode), the worker stops when it sees a message
    with an LSN exceeding the cutoff.
    """
    logger.info(f"Starting replay worker for topic '{config.replay_topic}' -> "
                f"[{config.target_db_table_schema}].[{config.target_db_table_name}]"
                f"{f' (cutoff: {cutoff_lsn})' if cutoff_lsn else ''}")

    delete_cnt: int = 0
    upsert_cnt: int = 0
    poll_time_acc: float = 0.0
    sql_time_acc: float = 0.0
    queue_get_wait: float = 0.0
    last_commit_time: datetime = datetime.now()
    last_consume_by_partition: Dict[int, Tuple[int, int]] = {}
    queued_deletes: Set[Any] = set()
    queued_upserts: Dict[Any, Tuple[str, List[Any]]] = {}
    proc_start_time: float = time.perf_counter()

    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient({'url': opts.schema_registry_url})
    avro_deserializer: AvroDeserializer = AvroDeserializer(schema_registry_client)
    pyodbc_conn_string = get_pyodbc_conn_string_from_opts(opts)
    progress_tracker = ProgressTracker(pyodbc_conn_string, opts.progress_tracking_table_schema,
                                       opts.progress_tracking_table_name, opts.all_changes_topic,
                                       opts.progress_tracking_namespace, proc_id)
    try:
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                          database=opts.target_db_database, timeout=15, login_timeout=15) as db_conn:
            # Create a modified opts object for this specific topic/table
            worker_opts = argparse.Namespace(**vars(opts))
            worker_opts.replay_topic = config.replay_topic
            worker_opts.target_db_table_schema = config.target_db_table_schema
            worker_opts.target_db_table_name = config.target_db_table_name
            worker_opts.cols_to_not_sync = config.cols_to_not_sync
            worker_opts.primary_key_fields_override = config.primary_key_fields_override

            # Initialize table metadata and create temp tables
            metadata = TableMetadata(config, db_conn)
            metadata.create_temp_tables(db_conn)

            # Truncate target table if requested and no prior progress exists
            existing_progress = progress_tracker.get_progress(
                worker_opts.target_db_table_schema, worker_opts.target_db_table_name, worker_opts.replay_topic)
            if opts.truncate_existing_data and not existing_progress:
                logger.info(f"Worker {config.replay_topic}: Truncating target table {metadata.fq_target_table_name} "
                           f"(no prior progress exists)")
                with db_conn.cursor() as cursor:
                    cursor.execute(f'TRUNCATE TABLE {metadata.fq_target_table_name};')
                db_conn.commit()
            elif opts.truncate_existing_data and existing_progress:
                logger.info(f"Worker {config.replay_topic}: Skipping truncation of {metadata.fq_target_table_name} "
                           f"(prior progress exists)")

            proc_start_time = time.perf_counter()
            logger.debug(f"Worker for '{config.replay_topic}': Listening for consumed messages.")

            while True:
                queue_get_wait_start = time.perf_counter()
                try:
                    topic, msg_partition, msg_offset, msg_timestamp, raw_key, raw_val = queue.get(timeout=0.1)
                except stdlib_queue.Empty:
                    topic, msg_partition, msg_offset, msg_timestamp, raw_key, raw_val = None, None, None, None, None, None
                queue_get_wait += time.perf_counter() - queue_get_wait_start

                # Check for EOF sentinel (topic is set but partition is None when consumer reached end of topic)
                # Note: when queue.get times out, topic is also None, so we check topic is not None
                reached_eof = topic is not None and msg_partition is None
                if reached_eof:
                    logger.info(f"Worker {config.replay_topic}: received EOF sentinel (topic has been fully consumed). "
                               f"Will flush and stop.")

                # Deserialize Avro messages in parallel workers
                msg_key: Optional[Dict[str, Any]] = None
                msg_val: Optional[Dict[str, Any]] = None
                if raw_key is not None:
                    # noinspection PyNoneFunctionAssignment
                    msg_key = avro_deserializer(raw_key, SerializationContext(topic, MessageField.KEY))  # type: ignore[func-returns-value]
                if raw_val is not None:
                    # noinspection PyNoneFunctionAssignment
                    msg_val = avro_deserializer(raw_val, SerializationContext(topic, MessageField.VALUE))  # type: ignore[func-returns-value]

                # Check cutoff LSN in backfill mode
                reached_cutoff = False
                if cutoff_lsn is not None and msg_val is not None:
                    msg_lsn: str = msg_val['__log_lsn']
                    if msg_lsn and msg_lsn > cutoff_lsn:
                        logger.info(f"Worker {config.replay_topic}: reached cutoff LSN at offset {msg_offset} "
                                   f"(msg LSN {msg_lsn} > cutoff {cutoff_lsn}). Will flush and stop.")
                        reached_cutoff = True
                        # Clear the current message so we don't process it, but continue to flush
                        msg_key = None
                        msg_val = None

                if reached_cutoff or reached_eof or len(queued_deletes) >= opts.delete_batch_size or \
                        len(queued_upserts) >= opts.upsert_batch_size or \
                        (datetime.now() - last_commit_time).seconds > opts.max_commit_latency_seconds:
                    with db_conn.cursor() as cursor:
                        if queued_deletes:
                            start_time = time.perf_counter()
                            db_conn.bulk_insert(metadata.delete_temp_table_name, list(queued_deletes))
                            sql_time_acc += time.perf_counter() - start_time
                            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            logger.info('Worker %s: Inserted to temp table for delete: %s items in %s ms',
                                        config.replay_topic, len(queued_deletes), elapsed_ms)

                            start_time = time.perf_counter()
                            cursor.execute(metadata.delete_stmt)
                            sql_time_acc += time.perf_counter() - start_time
                            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            logger.info('Worker %s: Deleted %s items in %s ms',
                                        config.replay_topic, len(queued_deletes), elapsed_ms)

                        if queued_upserts:
                            inserts: List[Any] = []
                            merges: List[Any] = []
                            for _, (op, val) in queued_upserts.items():
                                # CTDS unfortunately completely ignores values for target-table IDENTITY cols when doing
                                # a bulk_insert, so in that case we have to fall back to the slower MERGE mechanism:
                                if (not metadata.identity_col_name) and op == 'Insert':  # in ('Snapshot', 'Insert'): -- Snapshots can hit PK collisions! :(
                                    inserts.append(val)
                                else:
                                    merges.append(val)

                            # Inserts CAN be treated as merges, and it's probably more efficient to do so if there
                            # are relatively few of them, to cut down on DB round-trips:
                            if inserts and merges and len(inserts) < 100 or \
                                    len(inserts) / (len(inserts) + len(merges)) < 0.1:
                                logger.debug('Worker %s: Rolling %s inserts into %s merges',
                                             config.replay_topic, len(inserts), len(merges))
                                merges += inserts
                                inserts = []

                            if inserts:
                                start_time = time.perf_counter()
                                db_conn.bulk_insert(metadata.fq_target_table_name, inserts)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Worker %s: Inserted directly to target table: %s items in %s ms',
                                            config.replay_topic, len(inserts), elapsed_ms)

                            if merges:
                                start_time = time.perf_counter()
                                try:
                                    db_conn.bulk_insert(metadata.merge_temp_table_name, merges)
                                except _tds.DatabaseError as e:
                                    pprint.pprint(merges)
                                    raise e
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Worker %s: Inserted to temp table for merge: %s items in %s ms',
                                            config.replay_topic, len(merges), elapsed_ms)

                                start_time = time.perf_counter()
                                cursor.execute(metadata.merge_stmt)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Worker %s: Merged %s items in %s ms',
                                            config.replay_topic, len(merges), elapsed_ms)

                        if queued_upserts or queued_deletes:
                            queued_upserts.clear()
                            queued_deletes.clear()
                            progress_tracker.commit_progress(
                                worker_opts.target_db_table_schema, worker_opts.target_db_table_name,
                                worker_opts.replay_topic, last_consume_by_partition)
                            last_commit_time = datetime.now()

                    if reached_cutoff or reached_eof:
                        logger.info(f"Worker {config.replay_topic}: flushed remaining work, exiting.")
                        break

                    if stop_event.is_set() and queue.empty():
                        break

                if msg_key is None:
                    continue

                key_val: Tuple[Any, ...] = tuple((msg_key[x] for x in metadata.primary_key_field_names))

                if msg_val is None or msg_val['__operation'] == 'Delete':
                    queued_deletes.add(key_val)
                    queued_upserts.pop(key_val, None)
                    delete_cnt += 1
                else:
                    vals = metadata.convert_msg_to_row_values(msg_val, for_bcp=True)
                    queued_upserts[key_val] = (msg_val['__operation'], vals)
                    # Don't do this: deletes run first in the processing loop. Let that happen--if you don't, there
                    # is a chance that a delete-followed-by-insert happens in one batch and if you don't let that
                    # delete happen first, the insert will encounter a PK constraint violation:
                    #
                    # queued_deletes.discard(key_val)
                    upsert_cnt += 1

                if (len(queued_deletes) + len(queued_upserts)) % 5_000 == 0:
                    logger.debug('Worker %s: Currently have %s queued deletes and %s queued upserts. '
                                 'Time waiting on queue = %s. Apx. queue depth %s',
                                 config.replay_topic, len(queued_deletes), len(queued_upserts),
                                 queue_get_wait, queue.qsize())
                    queue_get_wait = 0

                last_consume_by_partition[msg_partition] = (msg_offset, msg_timestamp)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(f"Worker for '{config.replay_topic}' encountered error: {e}")
    finally:
        stop_event.set()
        logger.info(f"Worker for '{config.replay_topic}': Processed {delete_cnt} deletes, {upsert_cnt} upserts.")
        overall_time = time.perf_counter() - proc_start_time
        logger.info(f"Worker for '{config.replay_topic}' total times:\n"
                    f"Kafka poll: {poll_time_acc:.2f}s\nSQL execution: {sql_time_acc:.2f}s\n"
                    f"Overall: {overall_time:.2f}s")
