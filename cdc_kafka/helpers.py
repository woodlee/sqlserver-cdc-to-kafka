import datetime

import confluent_kafka


# Helper function for loggers working with Kafka messages
def format_coordinates(msg: confluent_kafka.Message) -> str:
    return f'{msg.topic()}:{msg.partition()}@{msg.offset()}, ' \
           f'time {datetime.datetime.fromtimestamp(msg.timestamp()[1] / 1000)}'
