import datetime
import logging

import pyodbc

logger = logging.getLogger(__name__)


class ClockSync(object):
    _instance = None
    SYNC_INTERVAL = datetime.timedelta(minutes=1)

    def __init__(self, db_conn: pyodbc.Connection):
        if ClockSync._instance is not None:
            raise Exception('ClockSync class should be used as a singleton.')

        self._last_sync_time = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        self._clock_skew = None
        self._db_conn = db_conn

        ClockSync._instance = self

    def db_time_to_utc(self, db_time: datetime.datetime) -> datetime.datetime:
        local_now = datetime.datetime.utcnow()
        if (local_now - self._last_sync_time) > ClockSync.SYNC_INTERVAL:
            with self._db_conn.cursor() as cursor:
                cursor.execute('SELECT GETDATE()')
                db_now = cursor.fetchval()
                self._clock_skew = local_now - db_now
            self._last_sync_time = local_now
            logger.debug('Current DB time: %s; local process UTC: %s; delta: %s', db_now, local_now, self._clock_skew)
        return db_time + self._clock_skew
