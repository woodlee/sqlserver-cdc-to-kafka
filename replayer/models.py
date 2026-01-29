"""Data models and constants for the replayer module."""

from datetime import datetime
from typing import Any, List, Optional, Tuple, Dict, NamedTuple


CDC_OPERATION_NAME_TO_ID = {
    'Snapshot': 0,
    'Delete': 1,
    'Insert': 2,
    'PreUpdate': 3,
    'PostUpdate': 4,
}


class LsnPosition(NamedTuple):
    """Represents an LSN position for comparison, using __log_lsn, __log_seqval, and operation.

    Comparison follows SQL Server CDC ordering: lsn first, then seqval, then operation.
    """
    lsn: bytes
    seqval: bytes
    operation: int

    @staticmethod
    def from_message(msg_val: Dict[str, Any]) -> 'LsnPosition':
        """Create an LsnPosition from a deserialized message value."""
        lsn_hex = msg_val.get('__log_lsn', '0x' + '00' * 10)
        seqval_hex = msg_val.get('__log_seqval', '0x' + '00' * 10)
        operation_name = msg_val.get('__operation', 'Snapshot')
        return LsnPosition(
            lsn=bytes.fromhex(lsn_hex[2:]),  # Strip '0x' prefix
            seqval=bytes.fromhex(seqval_hex[2:]),
            operation=CDC_OPERATION_NAME_TO_ID.get(operation_name, 0)
        )

    def __repr__(self) -> str:
        return f'LSN(0x{self.lsn.hex()}:{self.seqval.hex()}:{self.operation})'


class OrderedOperation(NamedTuple):
    """Represents a single CDC operation to be applied in strict order.

    Used in follow mode to maintain FK-safe ordering across tables.
    """
    original_topic: str  # The original single-table topic name
    operation_type: str  # 'delete' or 'upsert'
    key_val: Tuple[Any, ...]  # Primary key values
    row_values: Optional[List[Any]]  # Full row values for upserts, None for deletes
    cdc_operation: str  # Original CDC operation name (Insert, PostUpdate, Delete, etc.)


LOWEST_LSN_POSITION = LsnPosition(b'\x00' * 10, b'\x00' * 10, 0)
HIGHEST_LSN_POSITION = LsnPosition(b'\xff' * 10, b'\xff' * 10, 4)


class Progress(NamedTuple):
    """Represents a progress tracking record stored in the database."""
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
    """Configuration for replaying a single topic to a single table."""
    replay_topic: str
    target_db_table_schema: str
    target_db_table_name: str
    cols_to_not_sync: str
    primary_key_fields_override: str
