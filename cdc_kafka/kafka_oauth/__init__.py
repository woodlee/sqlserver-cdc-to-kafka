import argparse
import importlib
import os
from abc import ABC, abstractmethod

from typing import TypeVar, Type, Tuple, Optional

KafkaOauthProviderAbstractType = TypeVar('KafkaOauthProviderAbstractType', bound='KafkaOauthProviderAbstract')


class KafkaOauthProviderAbstract(ABC):
    @abstractmethod
    def consumer_oauth_cb(self, config_str: str) -> Tuple[str, float]:
        pass

    @abstractmethod
    def producer_oauth_cb(self, config_str: str) -> Tuple[str, float]:
        pass

    @abstractmethod
    def admin_oauth_cb(self, config_str: str) -> Tuple[str, float]:
        pass

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    @abstractmethod
    def construct_with_options(cls: Type[KafkaOauthProviderAbstractType],
                               opts: argparse.Namespace) -> KafkaOauthProviderAbstractType:
        pass


def add_kafka_oauth_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--kafka-oauth-provider',
                        default=os.environ.get('KAFKA_OAUTH_PROVIDER'),
                        help="A string of form <module_name>.<class_name> indicating an implementation of "
                             "kafka_oauth.KafkaOauthProviderAbstract that provides OAuth callback functions specified "
                             "when instantiating Kafka consumers, producers, or admin clients.")


def get_kafka_oauth_provider() -> Optional[KafkaOauthProviderAbstract]:
    parser = argparse.ArgumentParser()
    add_kafka_oauth_arg(parser)
    opts, _ = parser.parse_known_args()

    if not opts.kafka_oauth_provider:
        return None

    package_module, class_name = opts.kafka_oauth_provider.rsplit('.', 1)
    module = importlib.import_module(package_module)
    oauth_class: KafkaOauthProviderAbstract = getattr(module, class_name)
    oauth_class.add_arguments(parser)
    opts, _ = parser.parse_known_args()
    return oauth_class.construct_with_options(opts)
