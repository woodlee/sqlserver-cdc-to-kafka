import abc
import datetime
from typing import List, Any, Dict, Optional

import confluent_kafka
import pyodbc
import sortedcontainers

from .. import constants, sql_queries
from . import metrics

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .. import clock_sync


class AccumulatorAbstract(abc.ABC):
    @abc.abstractmethod
    def reset_and_start(self) -> None: pass

    @abc.abstractmethod
    def end_and_get_values(self) -> metrics.Metrics: pass

    @abc.abstractmethod
    def register_sleep(self, sleep_time_seconds: float) -> None: pass

    @abc.abstractmethod
    def register_db_query(self, seconds_elapsed: float, db_query_kind: str, retrieved_row_count: int) -> None: pass

    @abc.abstractmethod
    def register_kafka_produce(self, seconds_elapsed: float, original_value: Optional[Dict[str, Any]],
                               message_type: str) -> None: pass

    @abc.abstractmethod
    def kafka_delivery_callback(self, message_type: str, message: confluent_kafka.Message,
                                original_key: Dict[str, Any], original_value: Optional[Dict[str, Any]]) -> None: pass


class NoopAccumulator(AccumulatorAbstract):
    def reset_and_start(self) -> None: pass
    def end_and_get_values(self) -> metrics.Metrics: return metrics.Metrics()
    def register_sleep(self, sleep_time_seconds: float) -> None: pass
    def register_db_query(self, seconds_elapsed: float, db_query_kind: str, retrieved_row_count: int) -> None: pass

    def register_kafka_produce(self, seconds_elapsed: float, original_value: Optional[Dict[str, Any]],
                               message_type: str) -> None: pass
    def kafka_delivery_callback(self, message_type: str, message: confluent_kafka.Message,
                                original_key: Dict[str, Any], original_value: Optional[Dict[str, Any]]) -> None: pass


class Accumulator(AccumulatorAbstract):
    _instance = None

    def __init__(self, db_conn: pyodbc.Connection, clock_syncer: 'clock_sync.ClockSync',
                 metrics_namespace: str, process_hostname: str) -> None:
        if Accumulator._instance is not None:
            raise Exception('metric_reporting.Accumulator class should be used as a singleton.')

        self._db_conn: pyodbc.Connection = db_conn
        self._clock_syncer: 'clock_sync.ClockSync' = clock_syncer
        self._metrics_namespace: str = metrics_namespace
        self._process_hostname: str = process_hostname

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
        self._kafka_delivery_acks_count: int = 0
        self._produced_delete_changes_count: int = 0
        self._produced_insert_changes_count: int = 0
        self._produced_metadata_records_count: int = 0
        self._produced_snapshot_records_count: int = 0
        self._produced_deletion_tombstones_count: int = 0
        self._messages_copied_to_unified_topics: int = 0
        self._produced_update_changes_count: int = 0

    def end_and_get_values(self) -> metrics.Metrics:
        end_epoch_sec = datetime.datetime.utcnow().timestamp()
        interval_delta_sec = end_epoch_sec - self._interval_start_epoch_sec
        db_all_data_queries_total_time_sec = self._db_snapshot_queries_total_time_sec + \
            self._db_change_data_queries_total_time_sec
        db_all_data_queries_count = self._db_snapshot_queries_count + self._db_change_data_queries_count
        kafka_produces_count = self._produced_delete_changes_count + self._produced_insert_changes_count + \
            self._produced_metadata_records_count + self._produced_snapshot_records_count + \
            self._produced_deletion_tombstones_count + self._produced_update_changes_count + \
            self._messages_copied_to_unified_topics

        with self._db_conn.cursor() as cursor:
            q, _ = sql_queries.get_latest_cdc_entry_time()
            cursor.execute(q)
            cdc_lag = (datetime.datetime.utcnow() - self._clock_syncer.db_time_to_utc(cursor.fetchval())) \
                .total_seconds()

        m = metrics.Metrics()

        m.metrics_namespace = self._metrics_namespace
        m.process_hostname = self._process_hostname

        m.interval_start_epoch_sec = self._interval_start_epoch_sec
        m.interval_end_epoch_sec = end_epoch_sec
        m.interval_delta_sec = interval_delta_sec

        m.earliest_change_lsn_produced = \
            (self._change_lsns_produced and self._change_lsns_produced[0]) or None
        m.earliest_change_db_tran_end_time_produced = \
            (self._change_db_tran_end_times_produced and self._change_db_tran_end_times_produced[0]) \
            or None
        m.latest_change_lsn_produced = \
            (self._change_lsns_produced and self._change_lsns_produced[-1]) or None
        m.latest_change_db_tran_end_time_produced = \
            (self._change_db_tran_end_times_produced and self._change_db_tran_end_times_produced[-1]) \
            or None

        m.e2e_latency_avg_sec = \
            (self._e2e_latencies_sec and sum(self._e2e_latencies_sec) / len(self._e2e_latencies_sec)) or None
        e2e_max: Optional[float] = None
        e2e_min: Optional[float] = None
        if self._e2e_latencies_sec:
            e2e_max = max(self._e2e_latencies_sec)
            e2e_min = min(self._e2e_latencies_sec)
        m.e2e_latency_max_sec = e2e_max
        m.e2e_latency_min_sec = e2e_min

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

        m.produced_delete_changes_count = self._produced_delete_changes_count
        m.produced_insert_changes_count = self._produced_insert_changes_count
        m.produced_update_changes_count = self._produced_update_changes_count
        m.produced_snapshot_records_count = self._produced_snapshot_records_count
        m.produced_metadata_records_count = self._produced_metadata_records_count
        m.produced_deletion_tombstones_count = self._produced_deletion_tombstones_count
        m.messages_copied_to_unified_topics = self._messages_copied_to_unified_topics

        m.total_sleep_time_sec = self._total_sleep_time_sec

        return m

    def register_sleep(self, sleep_time_seconds: float) -> None:
        self._total_sleep_time_sec += sleep_time_seconds

    def register_db_query(self, seconds_elapsed: float, db_query_kind: str, retrieved_row_count: int) -> None:
        if db_query_kind == constants.SNAPSHOT_ROWS_KIND:
            self._db_snapshot_queries_count += 1
            self._db_snapshot_rows_retrieved_count += retrieved_row_count
            self._db_snapshot_queries_total_time_sec += seconds_elapsed
        elif db_query_kind == constants.CHANGE_ROWS_KIND:
            self._db_change_data_queries_count += 1
            self._db_change_data_rows_retrieved_count += retrieved_row_count
            self._db_change_data_queries_total_time_sec += seconds_elapsed
        else:
            raise Exception(f'Accumulator.register_db_query does not recognize db_query_kind "{db_query_kind}".')

    def register_kafka_produce(self, seconds_elapsed: float, original_value: Optional[Dict[str, Any]],
                               message_type: str) -> None:
        self._kafka_produces_total_time_sec += seconds_elapsed

        if message_type in (constants.CHANGE_PROGRESS_MESSAGE, constants.SNAPSHOT_PROGRESS_MESSAGE,
                            constants.METRIC_REPORTING_MESSAGE, constants.PROGRESS_DELETION_TOMBSTONE_MESSAGE):
            self._produced_metadata_records_count += 1
        elif message_type == constants.DELETION_CHANGE_TOMBSTONE_MESSAGE:
            self._produced_deletion_tombstones_count += 1
        elif message_type == constants.UNIFIED_TOPIC_CHANGE_MESSAGE:
            self._messages_copied_to_unified_topics += 1
        elif message_type == constants.SINGLE_TABLE_SNAPSHOT_MESSAGE:
            self._produced_snapshot_records_count += 1
        elif message_type == constants.SINGLE_TABLE_CHANGE_MESSAGE and original_value:
            self._change_lsns_produced.add(original_value[constants.LSN_NAME])
            self._change_db_tran_end_times_produced.add(original_value[constants.EVENT_TIME_NAME])
            operation_name = original_value[constants.OPERATION_NAME]
            if operation_name == constants.DELETE_OPERATION_NAME:
                self._produced_delete_changes_count += 1
            elif operation_name == constants.INSERT_OPERATION_NAME:
                self._produced_insert_changes_count += 1
            elif operation_name == constants.POST_UPDATE_OPERATION_NAME:
                self._produced_update_changes_count += 1
            else:
                raise Exception(f'Accumulator.register_kafka_produce does not recognize operation name: '
                                f'"{operation_name}".')
        else:
            raise Exception(f'Accumulator.register_kafka_produce does not recognize message type: "{message_type}".')

    def kafka_delivery_callback(self, message_type: str, message: confluent_kafka.Message,
                                original_key: Dict[str, Any], original_value: Optional[Dict[str, Any]]) -> None:
        self._kafka_delivery_acks_count += 1

        if message_type not in (constants.SINGLE_TABLE_CHANGE_MESSAGE, constants.UNIFIED_TOPIC_CHANGE_MESSAGE):
            return

        if not original_value:
            return

        timestamp_type, timestamp = message.timestamp()
        if timestamp_type != confluent_kafka.TIMESTAMP_CREATE_TIME:
            produce_datetime = datetime.datetime.utcnow()
        else:
            produce_datetime = datetime.datetime.utcfromtimestamp(timestamp / 1000.0)

        event_time = datetime.datetime.fromisoformat(original_value[constants.EVENT_TIME_NAME])
        db_commit_time = self._clock_syncer.db_time_to_utc(event_time)
        e2e_latency = (produce_datetime - db_commit_time).total_seconds()
        self._e2e_latencies_sec.append(e2e_latency)
