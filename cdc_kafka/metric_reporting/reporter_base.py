import argparse
import datetime
from abc import ABC, abstractmethod

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .metrics import Metrics


class ReporterBase(ABC):
    @abstractmethod
    def emit(self, metrics: 'Metrics') -> None:
        pass

    @abstractmethod
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        pass

    @abstractmethod
    def set_options(self, opts) -> None:
        pass

    @staticmethod
    def json_serialize_datetimes(obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))
