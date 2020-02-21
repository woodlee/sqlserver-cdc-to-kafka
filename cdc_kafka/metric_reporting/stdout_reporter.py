import argparse
import json
import logging

from . import reporter_base

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .metrics import Metrics

logger = logging.getLogger(__name__)


class StdoutReporter(reporter_base.ReporterBase):
    def emit(self, metrics: 'Metrics') -> None:
        logger.info('Metrics recorded in last interval: %s', json.dumps(
            metrics.as_dict(), default=StdoutReporter.json_serialize_datetimes))

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        pass

    def set_options(self, opts: argparse.Namespace) -> None:
        pass
