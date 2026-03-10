import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from confluent_kafka.admin import TopicMetadata
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .logging_config import get_logger

logger = get_logger(__name__)

UTC = timezone.utc


def build_consumer_config(kafka_bootstrap_servers: str, group_id: str, **overrides: Any) -> Dict[str, Any]:
    config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': group_id,
        'enable.auto.offset.store': False,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
        **overrides
    }
    return config


def commit_cb(err: KafkaError, tps: List[TopicPartition]) -> None:
    if err is not None:
        logger.error(f'Error committing offsets: {err}')
    else:
        logger.debug(f'Offsets committed for {tps}')


def format_coordinates(msg: Message) -> str:
    return f'{msg.topic()}:{msg.partition()}@{msg.offset()}, ' \
           f'time {datetime.fromtimestamp(msg.timestamp()[1] / 1000, UTC)}'


def get_latest_lsn_from_all_changes_topic(opts: argparse.Namespace) -> Tuple[str, int]:
    schema_registry_client = SchemaRegistryClient({'url': opts.schema_registry_url, 'timeout': 15.0})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer_conf = build_consumer_config(opts.kafka_bootstrap_servers,
                                          f'replayer-lsn-lookup-{int(datetime.now().timestamp())}',
                                          **opts.extra_kafka_consumer_config)
    consumer_conf['auto.offset.reset'] = 'latest'
    consumer = Consumer(consumer_conf)

    try:
        # Get topic metadata
        topics_meta: Dict[str, TopicMetadata] | None = consumer.list_topics(
            topic=opts.all_changes_topic).topics
        if topics_meta is None:
            raise Exception(f'No metadata found for topic {opts.all_changes_topic}')

        partitions = list((topics_meta[opts.all_changes_topic].partitions or {}).keys())
        if not partitions:
            raise Exception(f'No partitions found for topic {opts.all_changes_topic}')

        # For each partition, get the high watermark and read the last message
        latest_lsn = '0x00000000000000000000'
        latest_offset = -1

        for partition_id in partitions:
            tp = TopicPartition(opts.all_changes_topic, partition_id)
            low, high = consumer.get_watermark_offsets(tp)

            if high <= low:
                logger.debug(f'Partition {partition_id} is empty (low={low}, high={high})')
                continue

            # Seek to the last message
            tp = TopicPartition(opts.all_changes_topic, partition_id, high - 2)
            consumer.assign([tp])

            msg = consumer.poll(timeout=10.0)
            if msg is None:
                logger.warning(f'Could not read last message from partition {partition_id}')
                continue

            err = msg.error()
            if err:
                logger.warning(f'Error reading from partition {partition_id}: {err}')
                continue

            raw_val = msg.value()
            if raw_val is None:
                continue

            msg_val = avro_deserializer(raw_val, SerializationContext(opts.all_changes_topic, MessageField.VALUE))
            if msg_val is None:
                continue
            assert isinstance(msg_val, dict)

            offset = msg.offset()
            assert isinstance(offset, int)

            msg_lsn = msg_val['__log_lsn']
            if msg_lsn > latest_lsn:
                latest_lsn = msg_lsn
                latest_offset = offset

        if latest_offset < 0:
            raise Exception(f'Could not find any messages in topic {opts.all_changes_topic}')

        logger.info(f'Found latest LSN in all-changes topic: {latest_lsn} at offset {latest_offset}')
        return latest_lsn, latest_offset

    finally:
        consumer.close()
