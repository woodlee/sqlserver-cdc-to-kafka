import datetime
from functools import total_ordering
from typing import Tuple, Any, Dict, Optional

from . import change_index


@total_ordering
class ParsedRow(object):
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

    def __eq__(self, other) -> bool:
        if isinstance(other, ParsedRow):
            return (self.table_fq_name, self.value_dict) == (other.table_fq_name, other.value_dict)
        return False

    def __lt__(self, other: 'ParsedRow') -> bool:
        if other is None:
            return False

        if isinstance(other, ParsedRow):
            self_tuple = (
                self.change_idx or change_index.LOWEST_CHANGE_INDEX,
                self.event_db_time,
                self.table_fq_name
            )
            other_tuple = (
                other.change_idx or change_index.LOWEST_CHANGE_INDEX,
                other.event_db_time,
                other.table_fq_name
            )

            if self_tuple != other_tuple:
                return self_tuple < other_tuple

            # I know it seems backwards, but it's because we read snapshot rows backwards by their PKs:
            return self.ordered_key_field_values > other.ordered_key_field_values

        raise Exception(f'Cannot compare ParsedRow to object of type "{type(other)}"')

    def __repr__(self) -> str:
        return f'ParsedRow from {self.table_fq_name} of kind {self.row_kind}, change index {self.change_idx}'
