"""Progress tracking functions for the replayer module."""

import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import ctds  # type: ignore[import-untyped]

from .models import Progress

UTC = timezone.utc


def _upsert_progress_record(cursor: Any, progress_table_fq_name: str, source_topic_name: str,
                            source_topic_partition: int, target_table_schema: str, target_table_name: str,
                            offset: int, timestamp: datetime, namespace: str, proc_id: str,
                            use_object_id: bool = True) -> None:
    """Execute a MERGE statement to upsert a progress record.

    Args:
        cursor: Database cursor
        progress_table_fq_name: Fully qualified progress table name
        source_topic_name: Kafka topic name
        source_topic_partition: Kafka partition number
        target_table_schema: Target table schema (empty string for all-changes topic)
        target_table_name: Target table name (empty string for all-changes topic)
        offset: Last handled message offset
        timestamp: Last handled message timestamp
        namespace: Progress tracking namespace
        proc_id: Process ID
        use_object_id: If True, compute OBJECT_ID from schema+table; if False, use 0
    """
    if use_object_id:
        object_id_expr = "OBJECT_ID(:2 + '.' + :3)"
    else:
        object_id_expr = "0"

    cursor.execute(f'''
MERGE {progress_table_fq_name} AS pt
USING (SELECT
    :0 AS [source_topic_name]
    , :1 AS [source_topic_partition]
    , {object_id_expr} AS [target_table_object_id]
    , :2 AS [target_table_schema_name]
    , :3 AS [target_table_name]
    , :4 AS [last_handled_message_offset]
    , :5 AS [last_handled_message_timestamp]
    , GETDATE() AS [commit_time]
    , :6 AS [replayer_progress_namespace]
    , :7 AS [replayer_process_id]
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
    ''', (source_topic_name, source_topic_partition, target_table_schema, target_table_name,
          offset, timestamp, namespace, proc_id))


def get_progress(opts: argparse.Namespace, conn: ctds.Connection) -> List[Progress]:
    target_table_fq_name: str = \
        f'[{opts.target_db_table_schema.strip()}].[{opts.target_db_table_name.strip()}]'
    progress_table_fq_name: str = \
        f'[{opts.progress_tracking_table_schema.strip()}].[{opts.progress_tracking_table_name.strip()}]'

    with conn.cursor() as cursor:
        cursor.execute(f'''
IF OBJECT_ID(:0) IS NULL
BEGIN
    CREATE TABLE {progress_table_fq_name} (
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
        CONSTRAINT [PK_{opts.progress_tracking_table_name.strip()}] PRIMARY KEY CLUSTERED (
            [source_topic_name], [target_table_object_id], [replayer_progress_namespace], [source_topic_partition]
        )
    )
END
        ''', (progress_table_fq_name,))

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
FROM {progress_table_fq_name}
WHERE [source_topic_name] = :0
    AND [target_table_object_id] = OBJECT_ID(:1)
    AND [replayer_progress_namespace] = :2
        ''', (opts.replay_topic, target_table_fq_name, opts.progress_tracking_namespace))

        return [Progress(*row) for row in cursor.fetchall()]


def commit_progress(opts: argparse.Namespace, last_consume_by_partition: Dict[int, Tuple[int, int]],
                    proc_id: str, db_conn: ctds.Connection) -> None:
    """Commit progress for a single-table replay topic."""
    progress_table_fq_name: str = \
        f'[{opts.progress_tracking_table_schema.strip()}].[{opts.progress_tracking_table_name.strip()}]'

    with db_conn.cursor() as cursor:
        for partition, (offset, timestamp) in last_consume_by_partition.items():
            _upsert_progress_record(
                cursor, progress_table_fq_name, opts.replay_topic, partition,
                opts.target_db_table_schema, opts.target_db_table_name, offset,
                datetime.fromtimestamp(timestamp / 1000, UTC).replace(tzinfo=None),
                opts.progress_tracking_namespace, proc_id, use_object_id=True
            )
        db_conn.commit()


def get_all_changes_topic_progress(opts: argparse.Namespace, db_conn: ctds.Connection) -> Optional[Progress]:
    progress_table_fq_name = f'[{opts.progress_tracking_table_schema}].[{opts.progress_tracking_table_name}]'

    with db_conn.cursor() as cursor:
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
FROM {progress_table_fq_name}
WHERE [source_topic_name] = :0
    AND [replayer_progress_namespace] = :1
        ''', (opts.all_changes_topic, opts.progress_tracking_namespace))

        row = cursor.fetchone()
        if row:
            return Progress(*row)
        return None


def commit_all_changes_topic_progress(opts: argparse.Namespace, offset: int, timestamp: int,
                                      proc_id: str, db_conn: ctds.Connection) -> None:
    """Commit progress for the all-changes topic."""
    progress_table_fq_name = f'[{opts.progress_tracking_table_schema}].[{opts.progress_tracking_table_name}]'

    with db_conn.cursor() as cursor:
        _upsert_progress_record(
            cursor, progress_table_fq_name, opts.all_changes_topic, 0,
            '', '', offset,
            datetime.fromtimestamp(timestamp / 1000, UTC).replace(tzinfo=None),
            opts.progress_tracking_namespace, proc_id, use_object_id=False
        )
        db_conn.commit()
