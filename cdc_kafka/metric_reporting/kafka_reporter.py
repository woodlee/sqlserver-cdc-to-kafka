import argparse
import os

from . import reporter_base
from .. import kafka, constants

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .metrics import Metrics


class KafkaReporter(reporter_base.ReporterBase):
    DEFAULT_TOPIC = '_cdc_to_kafka_metrics'

    def __init__(self) -> None:
        self._metrics_topic: Optional[str] = None
        self._metrics_key_schema_id: Optional[int] = None
        self._metrics_value_schema_id: Optional[int] = None

    # noinspection PyProtectedMember
    def emit(self, metrics: 'Metrics') -> None:
        metrics_dict = metrics.as_dict()

        if self._metrics_key_schema_id is None:
            self._metrics_key_schema_id, self._metrics_value_schema_id = kafka.KafkaClient._instance.register_schemas(
                self._metrics_topic, metrics.METRICS_AVRO_KEY_SCHEMA, metrics.METRICS_AVRO_VALUE_SCHEMA)

        key = {'metrics_namespace': metrics_dict['metrics_namespace']}

        kafka.KafkaClient._instance.produce(
            self._metrics_topic, key, self._metrics_key_schema_id, metrics_dict, self._metrics_value_schema_id,
            constants.METRIC_REPORTING_MESSAGE)

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--kafka-metrics-topic',
                            default=os.environ.get('KAFKA_METRICS_TOPIC', KafkaReporter.DEFAULT_TOPIC),
                            help='Kafka topic to target when publishing process metrics metadata via the '
                                 f'KafkaReporter. Defaults to `{KafkaReporter.DEFAULT_TOPIC}`')

    def set_options(self, opts: argparse.Namespace) -> None:
        self._metrics_topic = opts.kafka_metrics_topic
