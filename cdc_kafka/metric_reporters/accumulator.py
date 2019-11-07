import datetime

import pyodbc

from cdc_kafka import constants


class MetricsAccumulator(object):
    def __init__(self, db_conn: pyodbc.Connection):
        self._db_conn = db_conn

        self.tombstone_publish = 0
        self.record_publish = 0
        self.cdc_lag_behind_now_ms = None
        self.app_lag_behind_cdc_ms = None

    def determine_lags(self, last_published_change_msg_db_time: datetime.datetime, any_tables_lagging: bool):
        with self._db_conn.cursor() as cursor:
            cursor.execute(constants.LAG_QUERY)
            latest_cdc_tran_end_time, db_lag_ms = cursor.fetchone()
        self.cdc_lag_behind_now_ms = db_lag_ms
        if not any_tables_lagging:
            self.app_lag_behind_cdc_ms = 0.0
        else:
            app_lag_ms = (latest_cdc_tran_end_time - last_published_change_msg_db_time).total_seconds() * 1000
            self.app_lag_behind_cdc_ms = max(app_lag_ms, 0.0)
