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
        if (datetime.datetime.utcnow() - self._last_sync_time) > ClockSync.SYNC_INTERVAL:
            with self._db_conn.cursor() as cursor:
                cursor.execute('SELECT GETDATE()')
                self._clock_skew = datetime.datetime.utcnow() - cursor.fetchval()
            self._last_sync_time = datetime.datetime.utcnow()
            logger.debug('Current DB time delta: %s', self._clock_skew)
        return db_time + self._clock_skew
