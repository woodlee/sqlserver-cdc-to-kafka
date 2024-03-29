import collections
import datetime
import logging
import multiprocessing as mp
import queue
import re
import struct
import time
from types import TracebackType
from typing import Any, Tuple, Dict, Optional, NamedTuple, List, Sequence, Type
from multiprocessing.synchronize import Event as EventClass

import pyodbc

from . import constants, helpers

logger = logging.getLogger(__name__)


class SQLQueryRequest(NamedTuple):
    queue_name: str
    query_metadata_to_reflect: Any
    query_text: str
    query_param_types: Sequence[Tuple[int, int, Optional[int]]]
    query_params: Sequence[Any]


class SQLQueryResult(NamedTuple):
    queue_name: str
    reflected_query_request_metadata: Any
    query_executed_utc: datetime.datetime
    query_took_sec: float
    result_rows: List[pyodbc.Row]
    query_params: Sequence[Any]


class SQLQueryProcessor(object):
    _instance = None

    def __init__(self, odbc_conn_string: str) -> None:
        if SQLQueryProcessor._instance is not None:
            raise Exception('SQLQueryProcessor class should be used as a singleton.')

        self._stop_event: EventClass = mp.Event()
        self._subprocess_request_queue: 'mp.Queue[SQLQueryRequest]' = mp.Queue(1000)
        self._subprocess_result_queue: 'mp.Queue[SQLQueryResult]' = mp.Queue(1000)
        self._output_queues: Dict[str, 'collections.deque[SQLQueryResult]'] = {}
        self._subprocess: mp.Process = mp.Process(target=query_processor, args=(
            odbc_conn_string, self._stop_event, self._subprocess_request_queue, self._subprocess_result_queue))
        self._results_wait_time: datetime.timedelta = datetime.timedelta(
            seconds=(constants.SQL_QUERY_RETRIES + 1) *
            (constants.SQL_QUERY_TIMEOUT_SECONDS + constants.SQL_QUERY_INTER_RETRY_INTERVAL_SECONDS)
        )
        self._ended: bool = False

        SQLQueryProcessor._instance = self

    def __enter__(self) -> 'SQLQueryProcessor':
        self._subprocess.start()
        logger.debug("SQL query subprocess started.")
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        if not self._ended:
            self._stop_event.set()
            self._check_if_ended()

    def _check_if_ended(self) -> bool:
        exitcode = self._subprocess.exitcode
        if (self._stop_event.is_set() or exitcode is not None) and not self._ended:
            logger.info('Ending SQL query subprocess...')
            self._ended = True
            if exitcode in (None, 0):
                pass
            elif exitcode == -9:
                logger.error('SQL query subprocess was killed by the OS! May need more memory...')
            else:
                logger.error('SQL query subprocess exited with nonzero status: %s', exitcode)
            self._stop_event.set()
            if self._subprocess.is_alive():
                self._subprocess.join(timeout=3)
            if self._subprocess.is_alive():
                logger.info('Forcing termination of SQL query subprocess.')
                self._subprocess.terminate()
                time.sleep(1)
            while not self._subprocess_request_queue.empty():
                try:
                    self._subprocess_request_queue.get_nowait()
                except (queue.Empty, EOFError):
                    break
            while not self._subprocess_result_queue.empty():
                try:
                    self._subprocess_result_queue.get_nowait()
                except (queue.Empty, EOFError):
                    break
            self._subprocess.close()
            self._subprocess_request_queue.close()
            self._subprocess_result_queue.close()
            logger.info("Done.")
        return self._ended

    def enqueue_query(self, request: 'SQLQueryRequest') -> None:
        if self._check_if_ended():
            return
        if request.queue_name not in self._output_queues:
            self._output_queues[request.queue_name] = collections.deque()
        self._subprocess_request_queue.put_nowait(request)

    def get_result(self, queue_name: str) -> Optional['SQLQueryResult']:
        if len(self._output_queues[queue_name]):
            return self._output_queues[queue_name].popleft()
        deadline = helpers.naive_utcnow() + self._results_wait_time
        while helpers.naive_utcnow() < deadline:
            try:
                res = self._subprocess_result_queue.get(timeout=0.1)
                if res.queue_name == queue_name:
                    return res
                self._output_queues[res.queue_name].append(res)
            except queue.Empty:
                pass
            if self._check_if_ended():
                return None
        return None


# This runs in the separate process, and therefore uses its own DB connection:
def query_processor(odbc_conn_string: str, stop_event: EventClass, request_queue: 'mp.Queue[SQLQueryRequest]',
                    result_queue: 'mp.Queue[SQLQueryResult]') -> None:
    try:
        with get_db_conn(odbc_conn_string) as db_conn:
            while not stop_event.is_set():
                try:
                    request = request_queue.get(block=True, timeout=0.5)
                except queue.Empty:
                    continue

                start_time = time.perf_counter()
                with db_conn.cursor() as cursor:
                    if request.query_param_types is not None:
                        cursor.setinputsizes(request.query_param_types)
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
                    result_rows = cursor.fetchall()
                query_took_sec = (time.perf_counter() - start_time)
                result_queue.put_nowait(SQLQueryResult(request.queue_name, request.query_metadata_to_reflect,
                                                       query_executed_utc, query_took_sec, result_rows,
                                                       request.query_params))
    except (KeyboardInterrupt, pyodbc.OperationalError) as exc:
        # 08S01 is the error code for "Communication link failure" which may be raised in response to KeyboardInterrupt
        if exc is pyodbc.OperationalError and not exc.args[0].startswith('08S01'):
            raise exc
    except Exception as exc:
        logger.exception('SQL query subprocess raised an exception.', exc_info=exc)
    finally:
        stop_event.set()
        result_queue.close()
        result_queue.join_thread()
        logger.info("SQL query subprocess exiting.")


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
