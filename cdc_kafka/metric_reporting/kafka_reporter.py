import argparse
import os

from . import reporter_base
from .. import kafka, constants

from typing import Type, TypeVar
from .metrics import Metrics

KafkaReporterType = TypeVar('KafkaReporterType', bound='KafkaReporter')


class KafkaReporter(reporter_base.ReporterBase):
    DEFAULT_TOPIC = '_cdc_to_kafka_metrics'

    def __init__(self, metrics_topic: str) -> None:
        self._metrics_topic: str = metrics_topic
        self._schemas_registered: bool = False
        self._metrics_key_schema_id: int = -1
        self._metrics_value_schema_id: int = -1

    # noinspection PyProtectedMember
    def emit(self, metrics: 'Metrics') -> None:
        metrics_dict = metrics.as_dict()
        key = {'metrics_namespace': metrics_dict['metrics_namespace']}

        if not self._schemas_registered:
            client = kafka.KafkaClient.get_instance()
            self._metrics_key_schema_id, self._metrics_value_schema_id = (client.register_schemas(
                self._metrics_topic, Metrics.METRICS_AVRO_KEY_SCHEMA, Metrics.METRICS_AVRO_VALUE_SCHEMA))
            self._schemas_registered = True

        kafka.KafkaClient.get_instance().produce(
            self._metrics_topic, key, self._metrics_key_schema_id, metrics_dict, self._metrics_value_schema_id,
            constants.METRIC_REPORTING_MESSAGE)

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--kafka-metrics-topic',
                            default=os.environ.get('KAFKA_METRICS_TOPIC', KafkaReporter.DEFAULT_TOPIC),
                            help='Kafka topic to target when publishing process metrics metadata via the '
                                 f'KafkaReporter. Defaults to `{KafkaReporter.DEFAULT_TOPIC}`')

    @classmethod
    def construct_with_options(cls: Type[KafkaReporterType], opts: argparse.Namespace) -> KafkaReporterType:
        metrics_topic: str = opts.kafka_metrics_topic or KafkaReporter.DEFAULT_TOPIC
        return cls(metrics_topic)
