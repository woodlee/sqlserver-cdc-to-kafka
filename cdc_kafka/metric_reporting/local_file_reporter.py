import argparse
import json
import logging
import os
import pathlib
from typing import TYPE_CHECKING, TypeVar, Type

from . import reporter_base

if TYPE_CHECKING:
    from .metrics import Metrics

logger = logging.getLogger(__name__)

LocalFileReporterType = TypeVar('LocalFileReporterType', bound='LocalFileReporter')


class LocalFileReporter(reporter_base.ReporterBase):
    def __init__(self, file_path: str) -> None:
        self._file_path: str = file_path
        pathlib.Path(file_path).touch()

    def emit(self, metrics: 'Metrics') -> None:
        with open(self._file_path, 'w') as target_file:
            json.dump(metrics.as_dict(), target_file, default=LocalFileReporter.json_serialize_datetimes)

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--metrics-file-path', default=os.environ.get('METRICS_FILE_PATH'),
                            help='Path to a file to which you want the process to write its most recent metrics.')

    @classmethod
    def construct_with_options(cls: Type[LocalFileReporterType], opts: argparse.Namespace) -> LocalFileReporterType:
        if not opts.metrics_file_path:
            raise Exception('LocalFileReporter cannot be used without specifying a value for METRICS_FILE_PATH')
        return cls(opts.metrics_file_path)
