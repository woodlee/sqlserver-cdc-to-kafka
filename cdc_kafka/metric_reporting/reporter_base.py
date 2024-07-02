import argparse
import datetime
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, TypeVar, Type

if TYPE_CHECKING:
    from .metrics import Metrics

ReporterBaseType = TypeVar('ReporterBaseType', bound='ReporterBase')


class ReporterBase(ABC):
    @abstractmethod
    def emit(self, metrics: 'Metrics') -> None:
        pass

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    @abstractmethod
    def construct_with_options(cls: Type[ReporterBaseType], opts: argparse.Namespace) -> ReporterBaseType:
        pass

    @staticmethod
    def json_serialize_datetimes(obj: object) -> Optional[str]:
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))
