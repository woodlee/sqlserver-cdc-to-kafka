import datetime
from typing import Tuple, Any, Dict, Optional

from . import change_index

class ParsedRow(object):
    __slots__ = 'table_fq_name', 'row_kind', 'operation_name', 'event_db_time', 'change_idx', \
        'ordered_key_field_values', 'destination_topic', 'avro_key_schema_id', 'avro_value_schema_id', 'key_dict', \
        'value_dict'

    def __init__(self, table_fq_name: str, row_kind: str, operation_name: str, event_db_time: datetime.datetime,
                 change_idx: Optional[change_index.ChangeIndex], ordered_key_field_values: Tuple[Any],
                 destination_topic: str, avro_key_schema_id: int, avro_value_schema_id: int,
                 key_dict: Dict[str, Any], value_dict: Dict[str, Any]) -> None:
        self.table_fq_name: str = table_fq_name
        self.row_kind: str = row_kind
        self.operation_name: str = operation_name
        self.event_db_time: datetime.datetime = event_db_time
        self.change_idx: Optional[change_index.ChangeIndex] = change_idx
        self.ordered_key_field_values: Tuple = ordered_key_field_values
        self.destination_topic: str = destination_topic
        self.avro_key_schema_id: int = avro_key_schema_id
        self.avro_value_schema_id: int = avro_value_schema_id
        self.key_dict: Dict[str, Any] = key_dict
        self.value_dict: Dict[str, Any] = value_dict

    def __repr__(self) -> str:
        return f'ParsedRow from {self.table_fq_name} of kind {self.row_kind}, change index {self.change_idx}'
