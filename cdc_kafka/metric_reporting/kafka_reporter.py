import argparse
import os
from typing import Type, TypeVar

from . import reporter_base
from .. import kafka, constants
from .metrics import Metrics
from ..serializers import SerializerAbstract
from ..serializers.avro import AvroSerializer

KafkaReporterType = TypeVar('KafkaReporterType', bound='KafkaReporter')


class KafkaReporter(reporter_base.ReporterBase):
    DEFAULT_TOPIC = '_cdc_to_kafka_metrics'

    def __init__(self, metrics_topic: str, opts: argparse.Namespace) -> None:
        self._metrics_topic: str = metrics_topic
        self._serializer: SerializerAbstract = AvroSerializer(
            opts.schema_registry_url, opts.always_use_avro_longs, opts.progress_topic_name,
            opts.snapshot_logging_topic_name, opts.metrics_topic_name, opts.avro_type_spec_overrides,
            disable_writes=True)

    # noinspection PyProtectedMember
    def emit(self, metrics: 'Metrics') -> None:
        metrics_dict = metrics.as_dict()
        key, value = self._serializer.serialize_metrics_message(metrics_dict['metrics_namespace'], metrics_dict)
        kafka.KafkaClient.get_instance().produce(self._metrics_topic, key, value, constants.METRIC_REPORTING_MESSAGE)

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--kafka-metrics-topic',
                            default=os.environ.get('KAFKA_METRICS_TOPIC', KafkaReporter.DEFAULT_TOPIC),
                            help='Kafka topic to target when publishing process metrics metadata via the '
                                 f'KafkaReporter. Defaults to `{KafkaReporter.DEFAULT_TOPIC}`')

    @classmethod
    def construct_with_options(cls: Type[KafkaReporterType], opts: argparse.Namespace) -> KafkaReporterType:
        metrics_topic: str = opts.kafka_metrics_topic or KafkaReporter.DEFAULT_TOPIC
        return cls(metrics_topic, opts)
