import datetime
from typing import Any, Sequence, List, Optional, Dict

from . import change_index


class ParsedRow(object):
    __slots__ = ('destination_topic', 'operation_id', 'cdc_update_mask', 'event_db_time',
                 'change_idx', 'ordered_key_field_values', 'table_data_cols', 'extra_headers')

    def __init__(self, destination_topic: str, operation_id: int, cdc_update_mask: bytes,
                 event_db_time: datetime.datetime, change_idx: change_index.ChangeIndex,
                 ordered_key_field_values: Sequence[Any], table_data_cols: List[Any],
                 extra_headers: Optional[Dict[str, str | bytes]] = None) -> None:
        self.destination_topic: str = destination_topic
        self.operation_id: int = operation_id
        self.cdc_update_mask: bytes = cdc_update_mask
        self.event_db_time: datetime.datetime = event_db_time
        self.change_idx: change_index.ChangeIndex = change_idx
        self.ordered_key_field_values: Sequence[Any] = ordered_key_field_values
        self.table_data_cols: List[Any] = table_data_cols
        self.extra_headers: Optional[Dict[str, str | bytes]] = extra_headers

    def __repr__(self) -> str:
        return f'ParsedRow for topic {self.destination_topic}, change index {self.change_idx}'
