import argparse
from abc import ABC, abstractmethod
from typing import TypeVar, Type, Tuple, Dict, Any, Optional, TYPE_CHECKING

import confluent_kafka

if TYPE_CHECKING:
    from ..parsed_row import ParsedRow
    from ..progress_tracking import ProgressEntry
    from ..tracked_tables import TrackedTable

SerializerAbstractType = TypeVar('SerializerAbstractType', bound='SerializerAbstract')


class DeserializedMessage(object):
    def __init__(self, raw_msg: confluent_kafka.Message, key_dict: Optional[Dict[str, Any]],
                 value_dict: Optional[Dict[str, Any]]):
        self.raw_msg = raw_msg
        self.key_dict = key_dict
        self.value_dict = value_dict


class SerializerAbstract(ABC):

    @abstractmethod
    def register_table(self, table: 'TrackedTable') -> None:
        pass

    @abstractmethod
    def serialize_table_data_message(self, row: 'ParsedRow') -> Tuple[bytes, bytes]:
        pass

    @abstractmethod
    def serialize_progress_tracking_message(self, progress_entry: 'ProgressEntry') -> Tuple[bytes, Optional[bytes]]:
        pass

    @abstractmethod
    def serialize_metrics_message(self, metrics_namespace: str, metrics: Dict[str, Any]) -> Tuple[bytes, bytes]:
        pass

    @abstractmethod
    def serialize_snapshot_logging_message(self, snapshot_log: Dict[str, Any]) -> Tuple[None, bytes]:
        pass

    @abstractmethod
    def deserialize(self, msg: confluent_kafka.Message) -> DeserializedMessage:
        pass

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    @abstractmethod
    def construct_with_options(cls: Type[SerializerAbstractType], opts: argparse.Namespace,
                               disable_writes: bool) -> SerializerAbstractType:
        pass


