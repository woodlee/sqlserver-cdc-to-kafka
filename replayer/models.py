from datetime import datetime
from typing import Any, List, Optional, Tuple, NamedTuple


class OrderedOperation(NamedTuple):
    original_topic: str
    cdc_operation: str
    key_val: Tuple[Any, ...]
    row_values: List[Any]
    offset: int
    timestamp: datetime
    updated_fields: Optional[List[str]] = None  # For PostUpdate: which fields actually changed


class Progress(NamedTuple):
    source_topic_name: str
    source_topic_partition: int
    target_table_object_id: int
    target_table_schema_name: str
    target_table_name: str
    last_handled_message_offset: int
    last_handled_message_timestamp: datetime
    commit_time: datetime
    replayer_progress_namespace: str
    replayer_process_id: str


class ReplayConfig(NamedTuple):
    replay_topic: str
    target_db_table_schema: str
    target_db_table_name: str
    cols_to_not_sync: str
    primary_key_fields_override: str
