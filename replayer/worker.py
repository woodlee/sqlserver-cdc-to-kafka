from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import pprint
import queue as stdlib_queue
import time
import traceback
from datetime import datetime
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event as EventClass
from typing import Any, Dict, List, Optional, Set, Tuple

import _tds  # type: ignore[import-not-found]
import ctds  # type: ignore[import-untyped]
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faster_fifo import Queue  # type: ignore[import-not-found]

from .backfill_progress import BackfillProgressTracker
from .logging_config import get_logger
from .models import ReplayConfig
from .progress import ProgressTracker
from .table_metadata import TableMetadata
from .utils import get_pyodbc_conn_string_from_opts

logger = get_logger(__name__)


def replay_worker(config: ReplayConfig, opts: argparse.Namespace, stop_event: EventClass, queue: Queue,
                  proc_id: str, cutoff_lsn: str = None,
                  processed_counter: Optional[Synchronized[int]] = None,
                  start_offsets_by_partition: Optional[Dict[int, int]] = None,
                  tables_complete_counter: Optional[Synchronized[int]] = None,
                  error_event: Optional[EventClass] = None,
                  error_queue: Optional[mp.Queue] = None) -> None:
    """Worker process that replays a single topic to a single table.

    If cutoff_lsn is provided (backfill mode), the worker stops when it sees a message
    with an LSN exceeding the cutoff.

    If processed_counter is provided, it will be incremented for each message processed
    to support backfill progress tracking. When start_offsets_by_partition is also provided,
    progress is tracked as offset advancement rather than message count, which correctly
    accounts for gaps in Kafka offsets.

    If tables_complete_counter is provided, it will be incremented when this worker finishes.
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

    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient({'url': opts.schema_registry_url,
                                                                         'timeout': 15.0})
    avro_deserializer: AvroDeserializer = AvroDeserializer(schema_registry_client)
    pyodbc_conn_string = get_pyodbc_conn_string_from_opts(opts)
    progress_tracker = ProgressTracker(pyodbc_conn_string, opts.progress_tracking_table_schema,
                                       opts.progress_tracking_table_name, opts.all_changes_topic,
                                       opts.progress_tracking_namespace, proc_id)
    try:
        with ctds.connect(opts.target_db_server, user=opts.target_db_user, password=opts.target_db_password,
                          database=opts.target_db_database, timeout=15, login_timeout=15, autocommit=True) as db_conn:
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
            last_worker_heartbeat = proc_start_time

            while True:
                current = time.perf_counter()
                if current - last_worker_heartbeat > 10.0:
                    logger.debug(f"Worker heartbeat. Observed apx queue size: {queue.qsize()}")
                    last_worker_heartbeat = current

                first_since_write = len(queued_upserts) + len(queued_deletes) <= 1
                if first_since_write:
                    logger.debug('Restarting accumulation phase')

                queue_get_wait_start = time.perf_counter()
                try:
                    topic, msg_partition, msg_offset, msg_timestamp, raw_key, raw_val = queue.get(timeout=1)
                except stdlib_queue.Empty:
                    topic, msg_partition, msg_offset, msg_timestamp, raw_key, raw_val = None, None, None, None, None, None
                took = time.perf_counter() - queue_get_wait_start
                if took > 1.1:
                    logger.debug('Long queue wait')
                queue_get_wait += took

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
                    retries: int = 0  # retries because schema registry calls very occasionally fail
                    while True:
                        try:
                            # noinspection PyNoneFunctionAssignment
                            msg_key = avro_deserializer(raw_key, SerializationContext(topic, MessageField.KEY))  # type: ignore[func-returns-value]
                            break
                        except Exception as e:
                            if retries >= 3:
                                raise e
                            logger.warning('Avro key deserialization failed. Retrying. Exception was: %s', str(e))
                            retries += 1
                            time.sleep(1)
                if raw_val is not None:
                    retries: int = 0  # retries because schema registry calls very occasionally fail
                    while True:
                        try:
                            # noinspection PyNoneFunctionAssignment
                            msg_val = avro_deserializer(raw_val, SerializationContext(topic, MessageField.VALUE))  # type: ignore[func-returns-value]
                            break
                        except Exception as e:
                            if retries >= 3:
                                raise e
                            logger.warning('Avro value deserialization failed. Retrying. Exception was: %s', str(e))
                            retries += 1
                            time.sleep(1)

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
                    logger.debug('Entering write loop. Reached cutoff: %s Reached EOF: %s Queued deletes: %s Queued '
                                 'upserts: %s Time since last commit: %s', reached_cutoff, reached_eof,
                                 len(queued_deletes), len(queued_upserts), (datetime.now() - last_commit_time).seconds)
                    with db_conn.cursor() as cursor:
                        if queued_deletes:
                            start_time = time.perf_counter()
                            db_conn.bulk_insert(metadata.delete_temp_table_name, list(queued_deletes))
                            sql_time_acc += time.perf_counter() - start_time
                            temp_table_elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            start_time = time.perf_counter()
                            cursor.execute(metadata.delete_stmt)
                            sql_time_acc += time.perf_counter() - start_time
                            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                            logger.info('Deleted %s items. Temp table insert: %s ms, delete stmt: %s ms',
                                        len(queued_deletes), temp_table_elapsed_ms, elapsed_ms)

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
                                logger.debug('Rolling %s inserts into %s merges', len(inserts), len(merges))
                                merges += inserts
                                inserts = []

                            if inserts:
                                start_time = time.perf_counter()
                                try:
                                    db_conn.bulk_insert(metadata.fq_target_table_name, inserts)
                                    sql_time_acc += time.perf_counter() - start_time
                                    elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                    logger.info('Inserted directly to target table: %s items in %s ms',
                                                len(inserts), elapsed_ms)
                                except _tds.IntegrityError as e:
                                    # Likely a PK constraint violation due to potential event replay after a process
                                    # restart; retry batch as merges
                                    logger.warning(f'Integrity error while directly inserting into '
                                                   f'{config.replay_topic}: {e}. Will retry operation as a MERGE...')
                                    merges = merges + inserts

                            if merges:
                                start_time = time.perf_counter()
                                try:
                                    db_conn.bulk_insert(metadata.merge_temp_table_name, merges)
                                except _tds.DatabaseError as e:
                                    pprint.pprint(merges)
                                    raise e
                                sql_time_acc += time.perf_counter() - start_time
                                temp_table_elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                start_time = time.perf_counter()
                                cursor.execute(metadata.merge_stmt)
                                sql_time_acc += time.perf_counter() - start_time
                                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                                logger.info('Merged %s items. Temp table insert: %s ms, merge stmt: %s ms',
                                            len(merges), temp_table_elapsed_ms, elapsed_ms)

                        if queued_upserts or queued_deletes:
                            queued_upserts.clear()
                            queued_deletes.clear()
                            progress_tracker.commit_progress(
                                worker_opts.target_db_table_schema, worker_opts.target_db_table_name,
                                worker_opts.replay_topic, last_consume_by_partition)
                            last_commit_time = datetime.now()

                        for partition, prev_offset in start_offsets_by_partition.items():
                            curr_offset = last_consume_by_partition.get(partition)
                            if curr_offset is not None:
                                offset_delta = curr_offset[0] - prev_offset
                                BackfillProgressTracker.increment_processed(processed_counter, offset_delta)
                                start_offsets_by_partition[partition] = curr_offset[0]
                        logger.debug('Incremented progress counters.')

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
        if error_queue is not None and error_event is not None:
            try:
                error_queue.put_nowait((config.replay_topic, traceback.format_exc()))
            except Exception:
                pass
            error_event.set()
        else:
            # Legacy fallback: if no error signaling available, set stop_event so consumer pauses
            stop_event.set()
    else:
        # Normal exit (cutoff reached, EOF, or clean stop): signal consumer to pause this topic
        stop_event.set()
    finally:
        if tables_complete_counter is not None:
            BackfillProgressTracker.increment_processed(tables_complete_counter)
        logger.info(f"Worker for '{config.replay_topic}': Processed {delete_cnt} deletes, {upsert_cnt} upserts.")
        overall_time = time.perf_counter() - proc_start_time
        logger.info(f"Worker for '{config.replay_topic}' total times:\n"
                    f"Kafka poll: {poll_time_acc:.2f}s\nSQL execution: {sql_time_acc:.2f}s\n"
                    f"Overall: {overall_time:.2f}s")
        logging.shutdown()
        time.sleep(0.5)
