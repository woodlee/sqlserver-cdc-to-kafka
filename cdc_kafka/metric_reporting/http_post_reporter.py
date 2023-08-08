import argparse
import json
import logging
import os
import threading

from jinja2 import Template
import requests

from . import reporter_base

from typing import TYPE_CHECKING, Optional, Dict, Any, TypeVar, Type

if TYPE_CHECKING:
    from .metrics import Metrics

logger = logging.getLogger(__name__)


HttpPostReporterType = TypeVar('HttpPostReporterType', bound='HttpPostReporter')


class HttpPostReporter(reporter_base.ReporterBase):
    def __init__(self, url: str, template: Optional[Template], headers: Dict[str, str]) -> None:
        self._url: str = url
        self._template: Optional[Template] = template
        self._headers: Dict[str, str] = headers

    def emit(self, metrics: 'Metrics') -> None:
        t = threading.Thread(target=self._post, args=(metrics.as_dict(),), name='HttpPostReporter')
        t.daemon = True
        t.start()

    def _post(self, metrics_dict: Dict[str, Any]) -> None:
        if self._template:
            body = self._template.render(metrics=metrics_dict)
        else:
            body = json.dumps(metrics_dict, default=HttpPostReporter.json_serialize_datetimes)

        try:
            resp = requests.post(self._url, data=body, headers=self._headers, timeout=10.0)
            resp.raise_for_status()
            logger.debug('Posted metrics to %s with code %s and response: %s', self._url, resp.status_code, resp.text)
        except requests.exceptions.RequestException as e:
            logger.warning('Failed to post metrics to %s: %s', self._url, e)

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--http-metrics-url', default=os.environ.get('HTTP_METRICS_URL'),
                            help='URL to target when publishing process metrics metadata via the HttpPostReporter.')
        parser.add_argument('--http-metrics-headers', default=os.environ.get('HTTP_METRICS_HEADERS'), type=json.loads,
                            help='Optional JSON object of HTTP headers k:v pairs to send along with the POST when '
                                 'publishing process metrics via the HttpPostReporter.')
        parser.add_argument('--http-metrics-template', default=os.environ.get('HTTP_METRICS_TEMPLATE'),
                            help='An optional Jinja2 template used to create the HTTP POST body when publishing '
                                 'process metrics via the HttpPostReporter. It may reference the fields defined in '
                                 'the metric_reporting.metrics.Metrics class.')

    @classmethod
    def construct_with_options(cls: Type[HttpPostReporterType], opts: argparse.Namespace) -> HttpPostReporterType:
        if not opts.http_metrics_url:
            raise Exception('HttpPostReporter cannot be used without specifying a value for HTTP_METRICS_URL')
        template = None
        if opts.http_metrics_template:
            template = Template(opts.http_metrics_template)
        return cls(opts.http_metrics_url, template, opts.http_metrics_headers)

