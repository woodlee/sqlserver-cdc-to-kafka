import argparse
from abc import ABC, abstractmethod

from . import accumulator


class BaseMetricReporter(ABC):
    @abstractmethod
    def emit(self, accum: accumulator.MetricsAccumulator):
        pass

    @abstractmethod
    def add_arguments(self, parser: argparse.ArgumentParser):
        pass

    @abstractmethod
    def set_options(self, opts):
        pass
