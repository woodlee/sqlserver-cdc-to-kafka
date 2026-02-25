from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyodbc

from .logging_config import get_logger
from .models import Progress

UTC = timezone.utc

logger = get_logger(__name__)


class ProgressTracker(object):
    def __init__(self, pyodbc_conn_string: str, progress_tracking_table_schema: str, progress_tracking_table_name: str,
                 all_changes_topic: str, progress_tracking_namespace: str, process_id: str,):
        self.progress_tracking_table_schema = progress_tracking_table_schema
        self.progress_tracking_table_name = progress_tracking_table_name
        self.all_changes_topic = all_changes_topic
        self.progress_tracking_namespace = progress_tracking_namespace
        self.process_id = process_id
        self.progress_table_fq_name = f'[{progress_tracking_table_schema}].[{progress_tracking_table_name}]'
        self.db_conn = pyodbc.connect(pyodbc_conn_string)

    def upsert_progress_record(self, cursor: Any, source_topic_name: str, source_topic_partition: int,
                               target_table_schema: str, target_table_name: str, offset: int, timestamp: datetime,
                               use_object_id: bool = True) -> None:
        if use_object_id:
            # Parameters 3 and 4 (0-indexed: target_table_schema and target_table_name) used in OBJECT_ID
            object_id_expr = "OBJECT_ID(? + '.' + ?)"
            params = (source_topic_name, source_topic_partition, target_table_schema, target_table_name,
                      target_table_schema, target_table_name, offset, timestamp,
                      self.progress_tracking_namespace, self.process_id)
        else:
            object_id_expr = "0"
            params = (source_topic_name, source_topic_partition, target_table_schema, target_table_name,
                      offset, timestamp, self.progress_tracking_namespace, self.process_id)

        cursor.execute(f'''
    MERGE {self.progress_table_fq_name} AS pt
    USING (SELECT
        ? AS [source_topic_name]
        , ? AS [source_topic_partition]
        , {object_id_expr} AS [target_table_object_id]
        , ? AS [target_table_schema_name]
        , ? AS [target_table_name]
        , ? AS [last_handled_message_offset]
        , ? AS [last_handled_message_timestamp]
        , GETDATE() AS [commit_time]
        , ? AS [replayer_progress_namespace]
        , ? AS [replayer_process_id]
    ) AS row ON (pt.[source_topic_name] = row.[source_topic_name]
        AND pt.[target_table_object_id] = row.[target_table_object_id]
        AND pt.[replayer_progress_namespace] = row.[replayer_progress_namespace]
        AND pt.[source_topic_partition] = row.[source_topic_partition]
    )
    WHEN MATCHED THEN UPDATE SET
        [last_handled_message_offset] = row.[last_handled_message_offset]
        , [last_handled_message_timestamp] = row.[last_handled_message_timestamp]
        , [commit_time] = row.[commit_time]
        , [replayer_process_id] = row.[replayer_process_id]
    WHEN NOT MATCHED THEN INSERT (
        [source_topic_name]
        , [source_topic_partition]
        , [target_table_object_id]
        , [target_table_schema_name]
        , [target_table_name]
        , [last_handled_message_offset]
        , [last_handled_message_timestamp]
        , [commit_time]
        , [replayer_progress_namespace]
        , [replayer_process_id]
    )
    VALUES (
        row.[source_topic_name]
        , row.[source_topic_partition]
        , row.[target_table_object_id]
        , row.[target_table_schema_name]
        , row.[target_table_name]
        , row.[last_handled_message_offset]
        , row.[last_handled_message_timestamp]
        , row.[commit_time]
        , row.[replayer_progress_namespace]
        , row.[replayer_process_id]
    );
        ''', params)

    def get_progress(self, target_db_table_schema: str, target_db_table_name: str,
                     replay_topic: str) -> List[Progress]:
        target_table_fq_name: str = \
            f'[{target_db_table_schema.strip()}].[{target_db_table_name.strip()}]'

        cursor = self.db_conn.cursor()
        try:
            cursor.execute(f'''
    IF OBJECT_ID(?) IS NULL
    BEGIN
        CREATE TABLE {self.progress_table_fq_name} (
            [source_topic_name]               VARCHAR(255) NOT NULL,
            [source_topic_partition]          INT          NOT NULL,
            [target_table_object_id]          INT          NOT NULL,
            [target_table_schema_name]        SYSNAME      NOT NULL,
            [target_table_name]               SYSNAME      NOT NULL,
            [last_handled_message_offset]     BIGINT,
            [last_handled_message_timestamp]  DATETIME2(3),
            [commit_time]                     DATETIME2(3),
            [replayer_progress_namespace]     VARCHAR(255) NOT NULL,
            [replayer_process_id]             VARCHAR(255) NOT NULL,
            CONSTRAINT [PK_{self.progress_tracking_table_name.strip()}] PRIMARY KEY CLUSTERED (
                [source_topic_name], [target_table_object_id], [replayer_progress_namespace], [source_topic_partition]
            )
        )
    END
            ''', (self.progress_table_fq_name,))
            self.db_conn.commit()

            cursor.execute(f'''
    SELECT [source_topic_name]
        , [source_topic_partition]
        , [target_table_object_id]
        , [target_table_schema_name]
        , [target_table_name]
        , [last_handled_message_offset]
        , [last_handled_message_timestamp]
        , [commit_time]
        , [replayer_progress_namespace]
        , [replayer_process_id]
    FROM {self.progress_table_fq_name}
    WHERE [source_topic_name] = ?
        AND [target_table_object_id] = OBJECT_ID(?)
        AND [replayer_progress_namespace] = ?
            ''', (replay_topic, target_table_fq_name, self.progress_tracking_namespace))

            return [Progress(*row) for row in cursor.fetchall()]
        finally:
            cursor.close()


    def commit_progress(self, target_db_table_schema: str, target_db_table_name: str,
                        replay_topic: str, last_consume_by_partition: Dict[int, Tuple[int, int]]) -> None:
        logger.debug(f'Committing progress for topic {replay_topic}: {str(last_consume_by_partition)}')
        with self.db_conn.cursor() as cursor:
            for partition, (offset, timestamp) in last_consume_by_partition.items():
                self.upsert_progress_record(
                    cursor, replay_topic, partition, target_db_table_schema, target_db_table_name,
                    offset, datetime.fromtimestamp(timestamp / 1000, UTC).replace(tzinfo=None), use_object_id=True
                )
            self.db_conn.commit()
        logger.debug(f'Progress commit complete.')


    def get_all_changes_topic_progress(self) -> Optional[Progress]:
        cursor = self.db_conn.cursor()
        try:
            cursor.execute(f'''
    SELECT [source_topic_name]
        , [source_topic_partition]
        , [target_table_object_id]
        , [target_table_schema_name]
        , [target_table_name]
        , [last_handled_message_offset]
        , [last_handled_message_timestamp]
        , [commit_time]
        , [replayer_progress_namespace]
        , [replayer_process_id]
    FROM {self.progress_table_fq_name}
    WHERE [source_topic_name] = ?
        AND [replayer_progress_namespace] = ?
            ''', (self.all_changes_topic, self.progress_tracking_namespace))

            row = cursor.fetchone()
            if row:
                return Progress(*row)
            return None
        finally:
            cursor.close()


    def commit_all_changes_topic_progress(self, offset: int, timestamp: datetime) -> None:
        cursor = self.db_conn.cursor()
        try:
            self.upsert_progress_record(
                cursor, self.all_changes_topic, 0, '', '', offset, timestamp, use_object_id=False
            )
            self.db_conn.commit()
        finally:
            cursor.close()

    def upsert_all_changes_progress(self, cursor: Any, offset: int, timestamp: datetime) -> None:
        """Upsert progress for the all-changes topic.

        This is designed to be called within an existing transaction - it does NOT commit.
        The caller is responsible for committing the transaction.
        """
        cursor.execute(f'''
    MERGE {self.progress_table_fq_name} AS pt
    USING (SELECT
        ? AS [source_topic_name]
        , ? AS [source_topic_partition]
        , 0 AS [target_table_object_id]
        , ? AS [target_table_schema_name]
        , ? AS [target_table_name]
        , ? AS [last_handled_message_offset]
        , ? AS [last_handled_message_timestamp]
        , GETDATE() AS [commit_time]
        , ? AS [replayer_progress_namespace]
        , ? AS [replayer_process_id]
    ) AS row ON (pt.[source_topic_name] = row.[source_topic_name]
        AND pt.[target_table_object_id] = row.[target_table_object_id]
        AND pt.[replayer_progress_namespace] = row.[replayer_progress_namespace]
        AND pt.[source_topic_partition] = row.[source_topic_partition]
    )
    WHEN MATCHED THEN UPDATE SET
        [last_handled_message_offset] = row.[last_handled_message_offset]
        , [last_handled_message_timestamp] = row.[last_handled_message_timestamp]
        , [commit_time] = row.[commit_time]
        , [replayer_process_id] = row.[replayer_process_id]
    WHEN NOT MATCHED THEN INSERT (
        [source_topic_name]
        , [source_topic_partition]
        , [target_table_object_id]
        , [target_table_schema_name]
        , [target_table_name]
        , [last_handled_message_offset]
        , [last_handled_message_timestamp]
        , [commit_time]
        , [replayer_progress_namespace]
        , [replayer_process_id]
    )
    VALUES (
        row.[source_topic_name]
        , row.[source_topic_partition]
        , row.[target_table_object_id]
        , row.[target_table_schema_name]
        , row.[target_table_name]
        , row.[last_handled_message_offset]
        , row.[last_handled_message_timestamp]
        , row.[commit_time]
        , row.[replayer_progress_namespace]
        , row.[replayer_process_id]
    );
        ''', (self.all_changes_topic, 0, '', '', offset, timestamp,
              self.progress_tracking_namespace, self.process_id))
