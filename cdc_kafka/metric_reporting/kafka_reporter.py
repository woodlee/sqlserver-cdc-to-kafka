import argparse
import logging
import os

from . import reporter_base
from .. import kafka

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .metrics import Metrics

logger = logging.getLogger(__name__)


class KafkaReporter(reporter_base.ReporterBase):
    DEFAULT_TOPIC = '_cdc_to_kafka_metrics'

    def __init__(self):
        self._metrics_topic = None
        self._metrics_key_schema_id, self._metrics_value_schema_id = None, None

    # noinspection PyProtectedMember
    def emit(self, metrics: 'Metrics') -> None:
        metrics_dict = metrics.as_dict()

        if self._metrics_key_schema_id is None:
            self._metrics_key_schema_id, self._metrics_value_schema_id = kafka.KafkaClient._instance.register_schemas(
                self._metrics_topic, metrics.AVRO_KEY_SCHEMA, metrics.AVRO_VALUE_SCHEMA)

        key = {'metrics_namespace': metrics_dict['metrics_namespace']}
        kafka.KafkaClient._instance.produce(
            self._metrics_topic, key, self._metrics_key_schema_id, metrics_dict, self._metrics_value_schema_id)

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--kafka-metrics-topic',
                            default=os.environ.get('KAFKA_METRICS_TOPIC', KafkaReporter.DEFAULT_TOPIC),
                            help='Kafka topic to target when publishing process metrics metadata via the '
                                 f'KafkaReporter. Defaults to `{KafkaReporter.DEFAULT_TOPIC}`')

    def set_options(self, opts) -> None:
        self._metrics_topic = opts.kafka_metrics_topic
