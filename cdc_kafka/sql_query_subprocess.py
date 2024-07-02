import collections
import datetime
import logging
import threading
import queue
import re
import struct
import time
from types import TracebackType
from typing import Any, Tuple, Dict, Optional, NamedTuple, List, Sequence, Type, Callable

import pyodbc

from . import constants, helpers, parsed_row

logger = logging.getLogger(__name__)


class SQLQueryRequest(NamedTuple):
    queue_name: str
    query_metadata_to_reflect: Any
    query_text: str
    query_param_types: Sequence[Tuple[int, int, Optional[int]]]
    query_params: Sequence[Any]
    parser: Callable[[pyodbc.Row], parsed_row.ParsedRow]


class SQLQueryResult(NamedTuple):
    queue_name: str
    reflected_query_request_metadata: Any
    query_executed_utc: datetime.datetime
    query_took_sec: float
    result_rows: List[parsed_row.ParsedRow]
    query_params: Sequence[Any]


class SQLQueryProcessor(object):
    _instance = None

    def __init__(self, odbc_conn_string: str) -> None:
        if SQLQueryProcessor._instance is not None:
            raise Exception('SQLQueryProcessor class should be used as a singleton.')

        self.odbc_conn_string: str = odbc_conn_string
        self._stop_event: threading.Event = threading.Event()
        self._request_queue: 'queue.Queue[SQLQueryRequest]' = queue.Queue(1000)
        self._output_queues: Dict[str, 'collections.deque[SQLQueryResult]'] = {}
        self._threads: List[threading.Thread] = [
            threading.Thread(target=self.querier_thread, name=f'sql-querier-{i + 1}')
            for i in range(constants.DB_QUERIER_CONCURRENT_THREADS)
        ]
        self._results_wait_time: datetime.timedelta = datetime.timedelta(
            seconds=(constants.SQL_QUERY_RETRIES + 1) *
            (constants.SQL_QUERY_TIMEOUT_SECONDS + constants.SQL_QUERY_INTER_RETRY_INTERVAL_SECONDS)
        )
        self._ended: bool = False

        SQLQueryProcessor._instance = self

    def __enter__(self) -> 'SQLQueryProcessor':
        for t in self._threads:
            t.start()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        if not self._ended:
            self._stop_event.set()
            self._check_if_ended()

    def _check_if_ended(self) -> bool:
        if self._stop_event.is_set() and not self._ended:
            logger.info('Ending SQL querier thread...')
            self._ended = True
            self._stop_event.set()
            for t in self._threads:
                if t.is_alive():
                    t.join(timeout=3)
            while not self._request_queue.empty():
                try:
                    self._request_queue.get_nowait()
                except (queue.Empty, EOFError):
                    break
            logger.info("Done.")
        return self._ended

    def enqueue_query(self, request: 'SQLQueryRequest') -> None:
        if self._check_if_ended():
            return
        if request.queue_name not in self._output_queues:
            self._output_queues[request.queue_name] = collections.deque()
        self._request_queue.put_nowait(request)

    def get_result(self, queue_name: str) -> Optional['SQLQueryResult']:
        deadline = helpers.naive_utcnow() + self._results_wait_time
        while helpers.naive_utcnow() < deadline:
            try:
                return self._output_queues[queue_name].popleft()
            except IndexError:
                time.sleep(0.001)
            if self._check_if_ended():
                return None
        return None

    # This runs in the separate thread, and uses its own DB connection:
    def querier_thread(self) -> None:
        try:
            with get_db_conn(self.odbc_conn_string) as db_conn:
                logger.debug("SQL querier thread started.")
                while not self._stop_event.is_set():
                    try:
                        request = self._request_queue.get(block=True, timeout=0.1)
                    except queue.Empty:
                        continue

                    start_time = time.perf_counter()
                    with db_conn.cursor() as cursor:
                        if request.query_param_types is not None:
                            cursor.setinputsizes(request.query_param_types)  # type: ignore[arg-type]
                        retry_count = 0
                        while True:
                            try:
                                if request.query_params is None:
                                    cursor.execute(request.query_text)
                                else:
                                    cursor.execute(request.query_text, request.query_params)
                                break
                            except pyodbc.OperationalError as exc:
                                # HYT00 is the error code for "Timeout expired"
                                if exc.args[0].startswith('HYT00') and retry_count < constants.SQL_QUERY_RETRIES:
                                    retry_count += 1
                                    logger.warning('SQL query timed out, retrying...')
                                    time.sleep(constants.SQL_QUERY_INTER_RETRY_INTERVAL_SECONDS)
                                    continue
                                raise exc
                        query_executed_utc = helpers.naive_utcnow()
                        result_rows = []
                        fetch_batch_count = constants.DB_FETCH_BATCH_SIZE
                        while fetch_batch_count >= constants.DB_FETCH_BATCH_SIZE:
                            fetch_batch_count = 0
                            for row in cursor.fetchmany(constants.DB_FETCH_BATCH_SIZE):
                                result_rows.append(request.parser(row))
                                fetch_batch_count += 1
                    query_took_sec = (time.perf_counter() - start_time)
                    self._output_queues[request.queue_name].append(
                        SQLQueryResult(request.queue_name, request.query_metadata_to_reflect, query_executed_utc,
                                       query_took_sec, result_rows, request.query_params)
                    )
        except pyodbc.OperationalError as exc:
            # 08S01 is the error code for "Communication link failure" which may be raised in response to KeyboardInterrupt
            if not exc.args[0].startswith('08S01'):
                raise exc
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            logger.exception('SQL querier thread raised an exception.', exc_info=exc)
        finally:
            self._stop_event.set()
            logger.info("SQL querier thread exiting.")


def get_db_conn(odbc_conn_string: str) -> pyodbc.Connection:
    # The Linux ODBC driver doesn't do failover, so we're hacking it in here. This will only work for initial
    # connections. If a failover happens while this process is running, the app will crash. Have a process supervisor
    # that can restart it if that happens, and it'll connect to the failover on restart:
    # THIS ASSUMES that you are using the exact keywords 'SERVER' and 'Failover_Partner' in your connection string!
    try:
        conn = pyodbc.connect(odbc_conn_string)
    except pyodbc.DatabaseError as e:
        server_match = re.match(r".*SERVER=(?P<hostname>.*?);", odbc_conn_string)
        failover_partner_match = re.match(r".*Failover_Partner=(?P<hostname>.*?);", odbc_conn_string)

        if failover_partner_match is None or server_match is None or e.args[0] not in ('42000', 'HYT00'):
            raise

        failover_partner = failover_partner_match.groups('hostname')[0]
        server = server_match.groups('hostname')[0]
        odbc_conn_string = odbc_conn_string.replace(server, failover_partner)
        logger.warning('Connection to PRIMARY failed, trying failover... (primary: "%s", failover: "%s")',
                       server, failover_partner)
        conn = pyodbc.connect(odbc_conn_string)

    def decode_truncated_utf16(raw_bytes: bytes) -> str:
        # SQL Server generally uses UTF-16-LE encoding for text. The length of NCHAR and NVARCHAR columns is the number
        # of byte pairs that can be stored in the column. But some higher UTF-16 codepoints are 4 bytes long. So it's
        # possible for a 4-byte character to get truncated halfway through, causing decode errors. This is to work
        # around that.
        try:
            return raw_bytes.decode("utf-16le")
        except UnicodeDecodeError as ex:
            return raw_bytes[:ex.start].decode("utf-16le")

    def decode_datetimeoffset(raw_bytes: bytes) -> datetime.datetime:
        tup = struct.unpack("<6hI2h", raw_bytes)
        return datetime.datetime(
            tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000,
            datetime.timezone(datetime.timedelta(hours=tup[7], minutes=tup[8]))
        )

    conn.add_output_converter(pyodbc.SQL_WVARCHAR, decode_truncated_utf16)
    conn.add_output_converter(pyodbc.SQL_WCHAR, decode_truncated_utf16)
    conn.add_output_converter(-155, decode_datetimeoffset)
    conn.timeout = constants.SQL_QUERY_TIMEOUT_SECONDS

    return conn
