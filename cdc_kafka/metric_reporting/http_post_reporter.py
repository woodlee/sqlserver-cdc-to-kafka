import argparse
import json
import logging
import os

from jinja2 import Template
import requests

from . import reporter_base

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .metrics import Metrics

logger = logging.getLogger(__name__)


class HttpPostReporter(reporter_base.ReporterBase):
    def __init__(self):
        self._url = None
        self._template = None
        self._headers = {}

    def emit(self, metrics: 'Metrics') -> None:
        if self._template:
            body = self._template.render(metrics=metrics.as_dict())
        else:
            body = json.dumps(metrics.as_dict(), default=HttpPostReporter.json_serialize_datetimes)

        resp = requests.post(self._url, data=body, headers=self._headers)
        resp.raise_for_status()

        logger.debug('Posted metrics to %s with response: %s', self._url, resp.text)

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--http-metrics-url', default=os.environ.get('HTTP_METRICS_URL'),
                            help='URL to target when publishing process metrics metadata via the HttpPostReporter.')
        parser.add_argument('--http-metrics-headers', default=os.environ.get('HTTP_METRICS_HEADERS'), type=json.loads,
                            help='Optional JSON object of HTTP headers to send along with the POST when publishing '
                                 'process metrics via the HttpPostReporter.')
        parser.add_argument('--http-metrics-template', default=os.environ.get('HTTP_METRICS_TEMPLATE'),
                            help='An optional Jinja2 template used to create the HTTP POST body when publishing '
                                 'process metrics via the HttpPostReporter. It may reference the fields defined in '
                                 'the metric_reporting.metrics.Metrics class.')

    def set_options(self, opts) -> None:
        if not opts.http_metrics_url:
            raise Exception('HttpPostReporter cannot be used without specifying a value for HTTP_METRICS_URL')

        self._url = opts.http_metrics_url

        if opts.http_metrics_template:
            self._template = Template(opts.http_metrics_template)

        if opts.http_metrics_headers:
            self._headers = opts.http_metrics_headers


