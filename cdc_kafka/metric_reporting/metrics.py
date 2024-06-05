from typing import Any, Dict


class Metrics(object):
    FIELDS_AND_TYPES = [
        ("metrics_namespace", "string"),
        ("process_hostname", "string"),

        ("interval_start_epoch_sec", "double"),
        ("interval_end_epoch_sec", "double"),
        ("interval_delta_sec", "float"),

        ("earliest_change_lsn_produced", ["null", "string"]),
        ("earliest_change_db_tran_end_time_produced", ["null", "string"]),
        ("latest_change_lsn_produced", ["null", "string"]),
        ("latest_change_db_tran_end_time_produced", ["null", "string"]),

        ("e2e_latency_avg_sec", ["null", "float"]),
        ("e2e_latency_max_sec", ["null", "float"]),
        ("e2e_latency_min_sec", ["null", "float"]),

        ("sql_server_cdc_process_lag_sec", ["null", "float"]),

        ("db_all_data_queries_avg_time_per_query_ms", ["null", "float"]),
        ("db_all_data_queries_count", "int"),
        ("db_all_data_queries_total_time_sec", "float"),
        ("db_all_data_rows_retrieved_count", "int"),

        ("db_change_data_queries_avg_time_per_query_ms", ["null", "float"]),
        ("db_change_data_queries_count", "int"),
        ("db_change_data_queries_total_time_sec", "float"),
        ("db_change_data_rows_retrieved_count", "int"),

        ("db_snapshot_queries_avg_time_per_query_ms", ["null", "float"]),
        ("db_snapshot_queries_count", "int"),
        ("db_snapshot_queries_total_time_sec", "float"),
        ("db_snapshot_rows_retrieved_count", "int"),

        ("kafka_produces_count", "int"),
        ("kafka_produces_total_time_sec", "float"),
        ("kafka_produces_avg_time_per_record_ms", ["null", "float"]),
        ("kafka_delivery_acks_count", "int"),

        ("produced_delete_changes_count", "int"),
        ("produced_insert_changes_count", "int"),
        ("produced_update_changes_count", "int"),
        ("produced_snapshot_records_count", "int"),
        ("produced_metadata_records_count", "int"),
        ("produced_deletion_tombstones_count", "int"),
        ("messages_copied_to_unified_topics", "int"),

        ("total_sleep_time_sec", "float"),
    ]

    FIELD_NAMES = {ft[0] for ft in FIELDS_AND_TYPES}

    def __setattr__(self, attr: str, value: Any) -> None:
        if attr not in Metrics.FIELD_NAMES:
            raise AttributeError(f'Metric name {attr} not recognized.')
        super(Metrics, self).__setattr__(attr, value)

    def as_dict(self) -> Dict[str, Any]:
        # Note that this will raise an exception if any of the expected metrics were not set on the object:
        return {fn: getattr(self, fn) for fn in Metrics.FIELD_NAMES}
