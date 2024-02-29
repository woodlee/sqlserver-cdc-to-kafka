import datetime

import confluent_kafka

from . import constants


# Helper function for loggers working with Kafka messages
def format_coordinates(msg: confluent_kafka.Message) -> str:
    return f'{msg.topic()}:{msg.partition()}@{msg.offset()}, ' \
           f'time {datetime.datetime.fromtimestamp(msg.timestamp()[1] / 1000, datetime.UTC)}'


def get_fq_change_table_name(capture_instance_name: str) -> str:
    assert '.' not in capture_instance_name
    capture_instance_name = capture_instance_name.strip(' []')
    return f'{constants.CDC_DB_SCHEMA_NAME}.{capture_instance_name}_CT'


def get_capture_instance_name(change_table_name: str) -> str:
    change_table_name = change_table_name.replace('[', '')
    change_table_name = change_table_name.replace(']', '')
    if change_table_name.startswith(constants.CDC_DB_SCHEMA_NAME + '.'):
        change_table_name = change_table_name.replace(constants.CDC_DB_SCHEMA_NAME + '.', '')
    assert change_table_name.endswith('_CT')
    return change_table_name[:-3]


def quote_name(name: str) -> str:
    name = name.replace('[', '')
    name = name.replace(']', '')
    parts = name.split('.')
    return '.'.join([f"[{p}]" for p in parts])


def naive_utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
