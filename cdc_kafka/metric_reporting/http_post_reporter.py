import argparse
import json
import logging
import os
import threading

from jinja2 import Template
import requests

from . import reporter_base

from typing import TYPE_CHECKING, Optional, Dict, Any

if TYPE_CHECKING:
    from .metrics import Metrics

logger = logging.getLogger(__name__)


class HttpPostReporter(reporter_base.ReporterBase):
    def __init__(self) -> None:
        self._url: Optional[str] = None
        self._template: Optional[Template] = None
        self._headers: Dict[str, str] = {}

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

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--http-metrics-url', default=os.environ.get('HTTP_METRICS_URL'),
                            help='URL to target when publishing process metrics metadata via the HttpPostReporter.')
        parser.add_argument('--http-metrics-headers', default=os.environ.get('HTTP_METRICS_HEADERS'), type=json.loads,
                            help='Optional JSON object of HTTP headers k:v pairs to send along with the POST when '
                                 'publishing process metrics via the HttpPostReporter.')
        parser.add_argument('--http-metrics-template', default=os.environ.get('HTTP_METRICS_TEMPLATE'),
                            help='An optional Jinja2 template used to create the HTTP POST body when publishing '
                                 'process metrics via the HttpPostReporter. It may reference the fields defined in '
                                 'the metric_reporting.metrics.Metrics class.')

    def set_options(self, opts: argparse.Namespace) -> None:
        if not opts.http_metrics_url:
            raise Exception('HttpPostReporter cannot be used without specifying a value for HTTP_METRICS_URL')

        self._url = opts.http_metrics_url

        if opts.http_metrics_template:
            self._template = Template(opts.http_metrics_template)

        if opts.http_metrics_headers:
            self._headers = opts.http_metrics_headers
