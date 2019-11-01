import datetime
from typing import List, Dict, Any

import pyodbc
import sortedcontainers

from .. import constants
from . import metrics

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .. import clock_sync


class Accumulator(object):
    _instance = None

    # In this class, all variables ending with _time represent cumulative totals in seconds

    def __init__(self, db_conn: pyodbc.Connection, clock_syncer: 'clock_sync.ClockSync',
                 metrics_namespace: str, process_hostname: str):
        if Accumulator._instance is not None:
            raise Exception('MetricsAccumulator class should be used as a singleton.')

        self._db_conn = db_conn
        self._clock_syncer = clock_syncer
        self._metrics_namespace = metrics_namespace
        self._process_hostname = process_hostname

        self.reset_and_start()

        Accumulator._instance = self

    # noinspection PyAttributeOutsideInit
    def reset_and_start(self) -> None:
        self._interval_start_epoch_sec: float = datetime.datetime.utcnow().timestamp()
        self._total_sleep_time_sec: float = 0
        self._db_change_data_queries_count: int = 0
        self._db_change_data_queries_total_time_sec: float = 0
        self._db_change_data_rows_retrieved_count: int = 0
        self._db_snapshot_queries_count: int = 0
        self._db_snapshot_queries_total_time_sec: float = 0
        self._db_snapshot_rows_retrieved_count: int = 0
        self._change_lsns_produced: sortedcontainers.SortedList = sortedcontainers.SortedList()
        self._change_db_tran_end_times_produced: sortedcontainers.SortedList = sortedcontainers.SortedList()
        self._e2e_latencies_sec: List[float] = []
        self._kafka_produces_total_time_sec: float = 0
        self._kafka_progress_commit_and_flush_total_time_sec: float = 0
        self._kafka_progress_commit_and_flush_count: int = 0
        self._kafka_delivery_acks_count: int = 0
        self._produced_delete_changes_count: int = 0
        self._produced_insert_changes_count: int = 0
        self._produced_metadata_records_count: int = 0
        self._produced_snapshot_records_count: int = 0
        self._produced_deletion_tombstones_count: int = 0
        self._produced_update_changes_count: int = 0

    def end_and_get_values(self) -> metrics.Metrics:
        end_epoch_sec = datetime.datetime.utcnow().timestamp()
        interval_delta_sec = end_epoch_sec - self._interval_start_epoch_sec
        db_all_data_queries_total_time_sec = self._db_snapshot_queries_total_time_sec + \
            self._db_change_data_queries_total_time_sec
        db_all_data_queries_count = self._db_snapshot_queries_count + self._db_change_data_queries_count
        kafka_produces_count = self._produced_delete_changes_count + self._produced_insert_changes_count + \
            self._produced_metadata_records_count + self._produced_snapshot_records_count + \
            self._produced_deletion_tombstones_count + self._produced_update_changes_count
        accounted_time_seconds = db_all_data_queries_total_time_sec + self._kafka_produces_total_time_sec + \
            self._kafka_progress_commit_and_flush_total_time_sec + self._total_sleep_time_sec

        with self._db_conn.cursor() as cursor:
            cursor.execute(constants.LATEST_CDC_ENTRY_TIME_QUERY)
            cdc_lag = (datetime.datetime.utcnow() - self._clock_syncer.db_time_to_utc(cursor.fetchval())) \
                .total_seconds()

        m = metrics.Metrics()

        m.metrics_namespace = self._metrics_namespace
        m.process_hostname = self._process_hostname

        m.interval_start_epoch_sec = self._interval_start_epoch_sec
        m.interval_end_epoch_sec = end_epoch_sec
        m.interval_delta_sec = interval_delta_sec

        m.earliest_change_lsn_produced = \
            (self._change_lsns_produced and f'0x{self._change_lsns_produced[0].hex()}') or None
        m.earliest_change_db_tran_end_time_produced = \
            (self._change_db_tran_end_times_produced and self._change_db_tran_end_times_produced[0]) or None
        m.latest_change_lsn_produced = \
            (self._change_lsns_produced and f'0x{self._change_lsns_produced[-1].hex()}') or None
        m.latest_change_db_tran_end_time_produced = \
            (self._change_db_tran_end_times_produced and self._change_db_tran_end_times_produced[-1]) or None

        m.e2e_latency_avg_sec = \
            (self._e2e_latencies_sec and sum(self._e2e_latencies_sec) / len(self._e2e_latencies_sec)) or None
        m.e2e_latency_max_sec = (self._e2e_latencies_sec and max(self._e2e_latencies_sec)) or None
        m.e2e_latency_min_sec = (self._e2e_latencies_sec and min(self._e2e_latencies_sec)) or None

        m.cdc_to_kafka_process_lag_sec = (self._e2e_latencies_sec and self._e2e_latencies_sec[-1] - cdc_lag) or None
        m.sql_server_cdc_process_lag_sec = cdc_lag

        m.db_all_data_queries_avg_time_per_query_ms = \
            (db_all_data_queries_count and
             db_all_data_queries_total_time_sec / db_all_data_queries_count * 1000) or None
        m.db_all_data_queries_count = db_all_data_queries_count
        m.db_all_data_queries_total_time_sec = db_all_data_queries_total_time_sec
        m.db_all_data_rows_retrieved_count = self._db_snapshot_rows_retrieved_count + \
            self._db_change_data_rows_retrieved_count

        m.db_change_data_queries_avg_time_per_query_ms = \
            (self._db_change_data_queries_count and
             self._db_change_data_queries_total_time_sec / self._db_change_data_queries_count * 1000) or None
        m.db_change_data_queries_count = self._db_change_data_queries_count
        m.db_change_data_queries_total_time_sec = self._db_change_data_queries_total_time_sec
        m.db_change_data_rows_retrieved_count = self._db_change_data_rows_retrieved_count

        m.db_snapshot_queries_avg_time_per_query_ms = \
            (self._db_snapshot_queries_count and
             self._db_snapshot_queries_total_time_sec / self._db_snapshot_queries_count * 1000) or None
        m.db_snapshot_queries_count = self._db_snapshot_queries_count
        m.db_snapshot_queries_total_time_sec = self._db_snapshot_queries_total_time_sec
        m.db_snapshot_rows_retrieved_count = self._db_snapshot_rows_retrieved_count

        m.kafka_produces_count = kafka_produces_count
        m.kafka_produces_total_time_sec = self._kafka_produces_total_time_sec
        m.kafka_produces_avg_time_per_record_ms = \
            (kafka_produces_count and
             self._kafka_produces_total_time_sec / kafka_produces_count * 1000) or None
        m.kafka_delivery_acks_count = self._kafka_delivery_acks_count

        m.kafka_progress_commit_and_flush_count = self._kafka_progress_commit_and_flush_count
        m.kafka_progress_commit_and_flush_total_time_sec = self._kafka_progress_commit_and_flush_total_time_sec
        m.kafka_progress_commit_and_flush_avg_time_ms = \
            (self._kafka_progress_commit_and_flush_count and
             self._kafka_progress_commit_and_flush_total_time_sec / self._kafka_progress_commit_and_flush_count
             * 1000) or None

        m.produced_delete_changes_count = self._produced_delete_changes_count
        m.produced_insert_changes_count = self._produced_insert_changes_count
        m.produced_update_changes_count = self._produced_update_changes_count
        m.produced_snapshot_records_count = self._produced_snapshot_records_count
        m.produced_metadata_records_count = self._produced_metadata_records_count
        m.produced_deletion_tombstones_count = self._produced_deletion_tombstones_count

        m.total_sleep_time_sec = self._total_sleep_time_sec
        m.unaccounted_time_sec = interval_delta_sec - accounted_time_seconds

        return m

    def register_sleep(self, sleep_time_seconds: float) -> None:
        self._total_sleep_time_sec += sleep_time_seconds

    def register_db_query(self, seconds_elapsed: float, is_snapshot: bool, rows_retrieved: int) -> None:
        if is_snapshot:
            self._db_snapshot_queries_count += 1
            self._db_snapshot_rows_retrieved_count += rows_retrieved
            self._db_snapshot_queries_total_time_sec += seconds_elapsed
        else:
            self._db_change_data_queries_count += 1
            self._db_change_data_rows_retrieved_count += rows_retrieved
            self._db_change_data_queries_total_time_sec += seconds_elapsed

    def register_kafka_produce(self, seconds_elapsed: float, is_metadata_msg: bool,
                               message_value: Dict[str, Any]) -> None:
        self._kafka_produces_total_time_sec += seconds_elapsed

        if message_value is None:
            self._produced_deletion_tombstones_count += 1
            return

        if is_metadata_msg:
            self._produced_metadata_records_count += 1
            return

        operation_name = message_value[constants.OPERATION_NAME]

        if operation_name == constants.SNAPSHOT_OPERATION_NAME:
            self._produced_snapshot_records_count += 1
            return

        self._change_lsns_produced.add(message_value[constants.LSN_NAME])
        self._change_db_tran_end_times_produced.add(message_value[constants.TRAN_END_TIME_NAME])

        if operation_name == constants.DELETE_OPERATION_NAME:
            self._produced_delete_changes_count += 1
        elif operation_name == constants.INSERT_OPERATION_NAME:
            self._produced_insert_changes_count += 1
        elif operation_name == constants.POST_UPDATE_OPERATION_NAME:
            self._produced_update_changes_count += 1
        else:
            raise Exception(f'Unrecognized operation name: {operation_name}')

    def register_kafka_commit(self, seconds_elapsed: float) -> None:
        self._kafka_progress_commit_and_flush_count += 1
        self._kafka_progress_commit_and_flush_total_time_sec += seconds_elapsed

    def register_kafka_delivery_callback(self, message_value: Dict[str, Any]) -> None:
        self._kafka_delivery_acks_count += 1
        if message_value[constants.OPERATION_NAME] != constants.SNAPSHOT_OPERATION_NAME:
            end_time = self._clock_syncer.db_time_to_utc(datetime.datetime.fromisoformat(
                message_value[constants.TRAN_END_TIME_NAME]))
            e2e_latency = (datetime.datetime.utcnow() - end_time).total_seconds()
            self._e2e_latencies_sec.append(e2e_latency)
