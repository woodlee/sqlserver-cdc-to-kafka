import argparse
import json
import logging

from . import reporter_base

from typing import TYPE_CHECKING, TypeVar, Type

if TYPE_CHECKING:
    from .metrics import Metrics

logger = logging.getLogger(__name__)

StdoutReporterType = TypeVar('StdoutReporterType', bound='StdoutReporter')


class StdoutReporter(reporter_base.ReporterBase):
    def emit(self, metrics: 'Metrics') -> None:
        logger.info('Metrics recorded in last interval: %s', json.dumps(
            metrics.as_dict(), default=StdoutReporter.json_serialize_datetimes))

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    def construct_with_options(cls: Type[StdoutReporterType], opts: argparse.Namespace) -> StdoutReporterType:
        return cls()
