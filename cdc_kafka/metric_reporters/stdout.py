import argparse
import logging

from . import accumulator, base


logger = logging.getLogger(__name__)


class StdoutReporter(base.BaseMetricReporter):
    def emit(self, accum: accumulator.MetricsAccumulator):
        logger.info('Published %s records in the last interval.', accum.record_publish)
        logger.info('Published %s deletion tombstones in the last interval.', accum.tombstone_publish)
        logger.info('Current lags in ms: DB: %s this app: %s', accum.cdc_lag_behind_now_ms, accum.app_lag_behind_cdc_ms)

    def add_arguments(self, parser: argparse.ArgumentParser):
        pass

    def set_options(self, opts):
        pass
