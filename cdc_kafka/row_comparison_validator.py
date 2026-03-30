import argparse
import datetime
import logging
import os
import re
import time
import zoneinfo
from typing import List, Optional, Tuple

import confluent_kafka
import pyodbc

from . import kafka, constants, options, sql_queries, helpers
from .change_index import ChangeIndex
from .metric_reporting import accumulator
from .serializers import SerializerAbstract

logger = logging.getLogger(__name__)

GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'
OK_TAG = f'{GREEN}[  OK]{RESET}'
FAIL_TAG = f'{RED}[FAIL]{RESET}'

PACIFIC_TZ = zoneinfo.ZoneInfo('America/Los_Angeles')
CLOCK_SKEW_BUFFER = datetime.timedelta(seconds=30)


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument('--topic-name-regex', required=True,
                   default=os.environ.get('TOPIC_NAME_REGEX'),
                   help='Regex with named capture groups "schema_name" and "table_name" '
                        'used to match Kafka topic names and extract the corresponding '
                        'source table identity.')


def get_matching_topics(kafka_client: kafka.KafkaClient,
                        topic_regex: re.Pattern[str]) -> List[Tuple[str, str, str]]:
    results: List[Tuple[str, str, str]] = []
    all_topics = kafka_client._cluster_metadata.topics or {}
    for topic_name in sorted(all_topics.keys()):
        m = topic_regex.match(topic_name)
        if m:
            try:
                schema_name = m.group('schema_name')
                table_name = m.group('table_name')
                results.append((topic_name, schema_name, table_name))
            except IndexError:
                logger.warning('Topic %s matched regex but missing named groups', topic_name)
    return results


def find_capture_instance(db_conn: pyodbc.Connection, schema_name: str,
                          table_name: str) -> Optional[str]:
    query, _ = sql_queries.get_cdc_capture_instances_metadata()
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        best_instance: Optional[str] = None
        best_date: Optional[datetime.datetime] = None
        for row in cursor.fetchall():
            if row.schema_name == schema_name and row.table_name == table_name:
                if best_date is None or row.create_date > best_date:
                    best_instance = row.capture_instance
                    best_date = row.create_date
    return best_instance


def fetch_change_table_rows(db_conn: pyodbc.Connection,
                            fq_change_table_name: str) -> List[ChangeIndex]:
    query, _ = sql_queries.get_change_table_rows_for_validation(
        fq_change_table_name, constants.VALIDATION_MAXIMUM_SAMPLE_SIZE_PER_TOPIC)
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        rows: List[ChangeIndex] = []
        for row in cursor.fetchall():
            rows.append(ChangeIndex(
                lsn=bytes(row[0]),
                command_id=row[1],
                seqval=bytes(row[2]),
                operation=row[3]
            ))
    return rows


def get_kafka_start_timestamp_ms(db_conn: pyodbc.Connection, first_lsn: bytes) -> int:
    query, param_specs = sql_queries.get_tran_end_time_for_lsn()
    with db_conn.cursor() as cursor:
        cursor.setinputsizes(param_specs)
        cursor.execute(query, first_lsn)
        row = cursor.fetchone()
    if row is None:
        raise Exception(f'No lsn_time_mapping entry found for LSN 0x{first_lsn.hex()}')

    naive_pacific: datetime.datetime = row[0]
    aware_pacific = naive_pacific.replace(tzinfo=PACIFIC_TZ)
    utc_dt = aware_pacific.astimezone(datetime.UTC)
    utc_dt = utc_dt - CLOCK_SKEW_BUFFER

    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
    return int((utc_dt - epoch).total_seconds() * 1000)


def _change_index_from_dict_legacy(source_dict: dict) -> ChangeIndex:
    """Build a ChangeIndex from a Kafka message value_dict that lacks __command_id."""
    return ChangeIndex(
        int(source_dict[constants.LSN_NAME][2:], 16).to_bytes(10, "big"),
        1,
        int(source_dict[constants.SEQVAL_NAME][2:], 16).to_bytes(10, "big"),
        constants.CDC_OPERATION_NAME_TO_ID[source_dict[constants.OPERATION_NAME]]
    )


def _change_indexes_match(expected: ChangeIndex, actual: ChangeIndex,
                          skip_command_id: bool) -> bool:
    if skip_command_id:
        return (expected.lsn == actual.lsn
                and expected.seqval == actual.seqval
                and expected.operation == actual.operation)
    return expected == actual


class ComparisonResult:
    def __init__(self) -> None:
        self.compared_count: int = 0
        self.legacy_mode: bool = False
        self.missing_tombstones: int = 0
        self.mismatch_block_sizes: List[int] = []
        self.first_mismatch_detail: Optional[str] = None
        self.error: Optional[str] = None  # for hard failures (timeout, EOF)

    @property
    def passed(self) -> bool:
        return (self.error is None
                and not self.mismatch_block_sizes
                and self.missing_tombstones == 0)

    def format_failure(self) -> str:
        parts: List[str] = []
        if self.error:
            parts.append(self.error)
        if self.mismatch_block_sizes:
            num_blocks = len(self.mismatch_block_sizes)
            total_rows = sum(self.mismatch_block_sizes)
            mn = min(self.mismatch_block_sizes)
            mx = max(self.mismatch_block_sizes)
            avg = total_rows / num_blocks
            parts.append(
                f'{num_blocks} mismatch block(s) ({total_rows} total rows; '
                f'block sizes min={mn}, max={mx}, avg={avg:.1f})')
            if self.first_mismatch_detail:
                parts.append(f'first: {self.first_mismatch_detail}')
        if self.missing_tombstones:
            parts.append(f'{self.missing_tombstones} missing tombstone(s)')
        return '; '.join(parts)


def consume_and_compare(
    kafka_client: kafka.KafkaClient,
    serializer: SerializerAbstract,
    topic_name: str,
    start_timestamp_ms: int,
    change_rows: List[ChangeIndex]
) -> ComparisonResult:
    result = ComparisonResult()

    part_count = kafka_client.get_topic_partition_count(topic_name)
    if not part_count:
        result.error = f'Topic {topic_name} does not exist or has no partitions'
        return result

    consumer = confluent_kafka.Consumer(kafka_client.consumer_config)

    try:
        topic_partitions = [
            confluent_kafka.TopicPartition(topic_name, p, start_timestamp_ms)
            for p in range(part_count)
        ]
        offset_results = consumer.offsets_for_times(
            topic_partitions, timeout=constants.KAFKA_REQUEST_TIMEOUT_SECS)

        for tp in offset_results:
            if tp.offset < 0:
                tp.offset = confluent_kafka.OFFSET_BEGINNING
        consumer.assign(offset_results)

        change_row_idx = 0
        expect_tombstone = False
        synced = False  # True once we've found the first change row's LSN in the topic
        eof_retried = False  # True after one retry pause at end-of-topic
        finished_parts = [False] * part_count
        legacy_mode: Optional[bool] = None  # detected on first compared message
        in_mismatch_block = False
        current_block_size = 0

        def _close_mismatch_block() -> None:
            nonlocal in_mismatch_block, current_block_size
            if in_mismatch_block:
                result.mismatch_block_sizes.append(current_block_size)
                in_mismatch_block = False
                current_block_size = 0

        while change_row_idx < len(change_rows):
            msg = consumer.poll(constants.KAFKA_REQUEST_TIMEOUT_SECS)

            if msg is None:
                _close_mismatch_block()
                result.error = (
                    f'Timed out waiting for Kafka messages at change row index '
                    f'{change_row_idx} ({change_rows[change_row_idx]})')
                break

            if msg.error():
                # noinspection PyProtectedMember
                if (msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF
                        and msg.partition() is not None):
                    finished_parts[msg.partition()] = True
                    if all(finished_parts):
                        remaining = len(change_rows) - change_row_idx
                        if remaining <= 10 and not eof_retried:
                            # Close to the goal — pause and retry in case the
                            # async CDC-to-Kafka process is still catching up
                            logger.info('Reached end of topic %s with %d rows '
                                        'remaining; pausing to wait for new '
                                        'messages...', topic_name, remaining)
                            time.sleep(3)
                            finished_parts = [False] * part_count
                            eof_retried = True
                            continue
                        _close_mismatch_block()
                        result.error = (
                            f'Reached end of all partitions but only compared '
                            f'{result.compared_count} of {len(change_rows)} '
                            f'change rows')
                        break
                    continue
                else:
                    raise confluent_kafka.KafkaException(msg.error())

            # noinspection PyArgumentList
            if msg.value() is None:
                if expect_tombstone:
                    expect_tombstone = False
                    continue
                # Unexpected tombstone — skip but don't fail (could be from prior
                # retention window or compaction)
                continue

            if expect_tombstone:
                # Missing tombstone — record it but continue processing this
                # message as a regular change message (don't consume it and
                # throw it away)
                result.missing_tombstones += 1
                expect_tombstone = False

            deser = serializer.deserialize(msg)
            if deser.value_dict is None:
                continue

            op_name = deser.value_dict.get(constants.OPERATION_NAME)
            if op_name == constants.SNAPSHOT_OPERATION_NAME:
                continue

            # Detect legacy messages (missing __command_id) on first compared message
            if legacy_mode is None:
                legacy_mode = constants.COMMAND_ID_NAME not in deser.value_dict
                result.legacy_mode = legacy_mode

            if legacy_mode:
                kafka_ci = _change_index_from_dict_legacy(deser.value_dict)
            else:
                kafka_ci = ChangeIndex.from_dict(deser.value_dict)

            # Skip messages preceding the first change table row (due to the
            # clock-skew buffer causing the consumer to start earlier than needed)
            if not synced:
                first_ci = change_rows[0]
                if legacy_mode:
                    # Zero out command_id on the DB side too so the comparison
                    # is apples-to-apples
                    first_ci = ChangeIndex(first_ci.lsn, 1, first_ci.seqval,
                                           first_ci.operation)
                if kafka_ci < first_ci:
                    continue
                synced = True

            expected_ci = change_rows[change_row_idx]
            if legacy_mode:
                expected_ci = ChangeIndex(expected_ci.lsn, 1, expected_ci.seqval,
                                          expected_ci.operation)

            if not _change_indexes_match(expected_ci, kafka_ci, legacy_mode):
                if not in_mismatch_block:
                    in_mismatch_block = True
                    current_block_size = 0
                    if result.first_mismatch_detail is None:
                        result.first_mismatch_detail = (
                            f'change row index {change_row_idx}: '
                            f'expected {expected_ci}, got {kafka_ci} '
                            f'from {helpers.format_coordinates(msg)}')
                current_block_size += 1
            else:
                _close_mismatch_block()

            result.compared_count += 1
            change_row_idx += 1

            if expected_ci.operation == constants.DELETE_OPERATION_ID:
                expect_tombstone = True

        _close_mismatch_block()

        # If the last matched row was a delete, try to consume the trailing tombstone
        if expect_tombstone:
            msg = consumer.poll(constants.KAFKA_REQUEST_TIMEOUT_SECS)
            # noinspection PyArgumentList
            if msg and not msg.error() and msg.value() is None:
                pass  # Got expected trailing tombstone
            elif msg and not msg.error() and msg.value() is not None:
                result.missing_tombstones += 1

        return result

    finally:
        consumer.close()


def main() -> None:
    opts, _, serializer = options.get_options_and_metrics_reporters(add_args)

    topic_regex = re.compile(opts.topic_name_regex)
    if 'schema_name' not in topic_regex.groupindex or 'table_name' not in topic_regex.groupindex:
        raise ValueError(
            '--topic-name-regex must contain named capture groups "schema_name" and "table_name"')

    db_conn = pyodbc.connect(opts.db_conn_string)

    try:
        with kafka.KafkaClient(accumulator.NoopAccumulator(), opts.kafka_bootstrap_servers,
                                opts.extra_kafka_consumer_config, {},
                                disable_writing=True) as kafka_client:

            matching_topics = get_matching_topics(kafka_client, topic_regex)

            if not matching_topics:
                print('No Kafka topics matched the provided regex.')
                return

            print(f'Found {len(matching_topics)} matching topic(s).\n')

            for topic_name, schema_name, table_name in matching_topics:
                fq_table = f'{schema_name}.{table_name}'

                capture_instance = find_capture_instance(db_conn, schema_name, table_name)
                if capture_instance is None:
                    print(f'{FAIL_TAG} {topic_name} ({fq_table}): '
                          f'No CDC capture instance found')
                    continue

                fq_change_table = helpers.get_fq_change_table_name(capture_instance)

                change_rows = fetch_change_table_rows(db_conn, fq_change_table)
                if not change_rows:
                    print(f'{OK_TAG} {topic_name} ({fq_table}): '
                          f'Change table is empty, nothing to compare')
                    continue

                try:
                    start_ts_ms = get_kafka_start_timestamp_ms(db_conn, change_rows[0].lsn)
                except Exception as e:
                    print(f'{FAIL_TAG} {topic_name} ({fq_table}): {e}')
                    continue

                cr = consume_and_compare(
                    kafka_client, serializer, topic_name, start_ts_ms, change_rows)

                legacy_note = (' (command_id not compared - legacy messages)'
                               if cr.legacy_mode else '')

                if cr.passed:
                    print(f'{OK_TAG} {topic_name} ({fq_table}): '
                          f'{cr.compared_count} messages compared '
                          f'successfully{legacy_note}')
                else:
                    print(f'{FAIL_TAG} {topic_name} ({fq_table}): '
                          f'{cr.format_failure()} '
                          f'({cr.compared_count} compared){legacy_note}')
    finally:
        db_conn.close()


if __name__ == "__main__":
    # importing this file to pick up the logging config in __init__; is there a better way??
    # noinspection PyUnresolvedReferences
    from cdc_kafka import row_comparison_validator
    row_comparison_validator.main()
