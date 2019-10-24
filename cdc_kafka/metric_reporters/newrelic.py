import argparse
import logging
import os

import newrelic.agent

from . import accumulator, base


logger = logging.getLogger(__name__)


class NewRelicReporter(base.BaseMetricReporter):
    def __init__(self):
        self._license_key = None
        self._nr_app_name = None
        self._nr_event_name = None
        self._nr_app = None

    def emit(self, accum: accumulator.MetricsAccumulator):
        newrelic.agent.record_custom_event(self._nr_event_name, {
            'app_lag_ms': accum.app_lag_behind_cdc_ms,
            'db_cdc_lag_ms': accum.cdc_lag_behind_now_ms,
            'records_published': accum.record_publish,
        }, application=self._nr_app)

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument('--new-relic-license-key', default=os.environ.get('NEW_RELIC_LICENSE_KEY'))
        parser.add_argument('--new-relic-app-name', default=os.environ.get('NEW_RELIC_APP_NAME'))
        parser.add_argument('--new-relic-event-name',
                            default=os.environ.get('NEW_RELIC_EVENT_NAME', 'cdc_to_kafka_metrics'))

    def set_options(self, opts):
        self._license_key = opts.new_relic_license_key
        self._nr_app_name = opts.new_relic_app_name
        self._nr_event_name = opts.new_relic_event_name

        if not self._license_key:
            raise Exception('NewRelicReporter needs a license key')
        if not self._nr_app_name:
            raise Exception('NewRelicReporter needs an app name')

        with open('newrelic.ini', 'w') as ini_file:
            ini_file.write(f'''
[newrelic]

license_key = {self._license_key}
app_name = {self._nr_app_name}
monitor_mode = true
log_level = debug
log_file = /var/log/newrel.log
high_security = false
error_collector.enabled = true
error_collector.ignore_errors = false
browser_monitoring.auto_instrument = false
thread_profiler.enabled = false
distributed_tracing.enabled = false            
            ''')
        import newrelic.agent
        newrelic.agent.initialize('newrelic.ini')
        self._nr_app = newrelic.agent.application(name=self._nr_app_name)

