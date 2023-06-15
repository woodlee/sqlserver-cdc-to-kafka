import datetime
import logging
from typing import Optional

import pyodbc

from . import sql_queries, constants

logger = logging.getLogger(__name__)


class ClockSync(object):
    _instance = None

    def __init__(self, db_conn: pyodbc.Connection) -> None:
        if ClockSync._instance is not None:
            raise Exception('ClockSync class should be used as a singleton.')

        self._last_sync_time: datetime.datetime = datetime.datetime.utcnow() - 2 * constants.DB_CLOCK_SYNC_INTERVAL
        self._clock_skew: Optional[datetime.timedelta] = None
        self._db_conn: pyodbc.Connection = db_conn

        ClockSync._instance = self

    def db_time_to_utc(self, db_time: datetime.datetime) -> datetime.datetime:
        now = datetime.datetime.utcnow()
        if (now - self._last_sync_time) > constants.DB_CLOCK_SYNC_INTERVAL:
            with self._db_conn.cursor() as cursor:
                q, _ = sql_queries.get_date()
                cursor.execute(q)
                db_now = cursor.fetchval()
                self._clock_skew = now - db_now
            self._last_sync_time = now
            logger.debug('Current DB time: %s; local process UTC: %s; delta: %s', db_now, now, self._clock_skew)
        return db_time + self._clock_skew
