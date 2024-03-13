import argparse
import datetime
import logging
import os
from typing import Tuple, TypeVar, Type

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from . import KafkaOauthProviderAbstract

logger = logging.getLogger(__name__)

AwsMskOauthCallbackProviderType = TypeVar('AwsMskOauthCallbackProviderType', bound='AwsMskOauthCallbackProvider')


class AwsMskOauthCallbackProvider(KafkaOauthProviderAbstract):
    def __init__(self, aws_region: str):
        self.aws_region: str = aws_region
        self._auth_token: str = ''
        self._expiry_ts: float = datetime.datetime.now(datetime.timezone.utc).timestamp()

    def consumer_oauth_cb(self, config_str: str) -> Tuple[str, float]:
        return self._common_cb()

    def producer_oauth_cb(self, config_str: str) -> Tuple[str, float]:
        return self._common_cb()

    def admin_oauth_cb(self, config_str: str) -> Tuple[str, float]:
        return self._common_cb()

    def _common_cb(self) -> Tuple[str, float]:
        if not self._auth_token or datetime.datetime.now(datetime.timezone.utc).timestamp() > self._expiry_ts:
            self._auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(self.aws_region)
            self._expiry_ts = expiry_ms / 1000
            logger.debug('AwsMskOauthCallbackProvider generated an auth token that expires at %s',
                         datetime.datetime.fromtimestamp(self._expiry_ts, datetime.timezone.utc))
        return self._auth_token, self._expiry_ts

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--aws-region', default=os.environ.get('AWS_REGION'),
                            help='AWS region name to use for IAM-based authentication to an AWS MSK cluster.')
        parser.add_argument('--aws-role-session-name', default=os.environ.get('AWS_ROLE_SESSION_NAME'),
                            help='A session name for the process to maintain principal-name stability when'
                                 're-authenticating for AWS IAM/SASL')

    @classmethod
    def construct_with_options(cls: Type[AwsMskOauthCallbackProviderType],
                               opts: argparse.Namespace) -> AwsMskOauthCallbackProviderType:
        if not opts.aws_region:
            raise Exception('AwsMskOauthCallbackProvider cannot be used without specifying a value for AWS_REGION')
        return cls(opts.aws_region)
