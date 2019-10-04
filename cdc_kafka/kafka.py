import collections
import datetime
import json
import logging
import socket
from typing import List, Dict

import avro
import confluent_kafka.avro

logger = logging.getLogger(__name__)


class KafkaClient(object):
    def __init__(self, bootstrap_servers: str, schema_registry_url: str, kafka_timeout_seconds: int,
                 extra_kafka_consumer_config: List[str], extra_kafka_producer_config: List[str]):
        consumer_config = {'bootstrap.servers': bootstrap_servers,
                           'group.id': f'cdc_to_kafka_progress_check_{socket.getfqdn()}',
                           'schema.registry.url': schema_registry_url}
        producer_config = {'bootstrap.servers': bootstrap_servers,
                           'linger.ms': '20',
                           'schema.registry.url': schema_registry_url}

        for extra in extra_kafka_consumer_config or []:
            k, v = extra.split(':')
            consumer_config[k] = v

        for extra in extra_kafka_producer_config or []:
            k, v = extra.split(':')
            producer_config[k] = v

        logger.debug('Kafka consumer configuration: \n%s', json.dumps(consumer_config, indent=4))
        logger.debug('Kafka producer configuration: \n%s', json.dumps(producer_config, indent=4))

        self.consumer = confluent_kafka.avro.AvroConsumer(consumer_config)
        self.producer = confluent_kafka.avro.AvroProducer(producer_config)

        self.kafka_timeout_seconds = kafka_timeout_seconds

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        logger.info("Closing Kafka consumer and flushing Kafka producer...")
        self.consumer.close()
        self.producer.flush(self.kafka_timeout_seconds)
        logger.info("Done.")

    successful_delivery_counter = 0
    last_producer_poll_time = datetime.datetime.now()

    @staticmethod
    def on_delivery_cb(err: confluent_kafka.KafkaError, msg: confluent_kafka.Message) -> None:
        if err is not None:
            logger.error(f'Message delivery failed for topic {msg.topic()} with error: {err}')
        else:
            KafkaClient.successful_delivery_counter += 1
            if KafkaClient.successful_delivery_counter % 1000 == 0:
                logger.debug(f'{KafkaClient.successful_delivery_counter} messages successfully produced to Kafka '
                             f'so far.')

    def produce(self, topic: str, key: Dict[str, object], key_schema: avro.schema.RecordSchema,
                value: Dict[str, object], value_schema: avro.schema.RecordSchema) -> None:
        if datetime.datetime.now() - KafkaClient.last_producer_poll_time > datetime.timedelta(seconds=1):
            # This poll is what triggers the on_delivery_cb for messages produced since the prior poll
            self.producer.poll(self.kafka_timeout_seconds)
            KafkaClient.last_producer_poll_time = datetime.datetime.now()
        self.producer.produce(topic=topic, key=key, key_schema=key_schema, value=value, value_schema=value_schema,
                              callback=KafkaClient.on_delivery_cb)

    # Returns dict where key is topic name and value is list of the last message from each of its partitions:
    def get_last_messages_for_topics(self, topic_names: List[str]) -> Dict[str, List[confluent_kafka.Message]]:
        cluster_metadata = self.consumer.list_topics(timeout=self.kafka_timeout_seconds)
        latest_messages_by_topic = collections.defaultdict(list)

        for topic_name in topic_names:
            topic_metadata = cluster_metadata.topics.get(topic_name)

            if topic_metadata is None:
                logger.warning('Topic name %s was not found in Kafka. This process will create it if corresponding '
                               'CDC entries are found.', topic_name)
                continue

            offset_assignments = []
            partition_count = len(topic_metadata.partitions)

            for partition_ix in range(partition_count):
                topic_partition = confluent_kafka.TopicPartition(topic_name, partition_ix)
                lo, hi = self.consumer.get_watermark_offsets(topic_partition, timeout=self.kafka_timeout_seconds)

                logger.debug('Watermark offsets for topic %s: low @ %s, high @ %s', topic_name, lo, hi)

                if lo == hi:
                    # the partition is empty (either zero-offset, or has been fully truncated):
                    continue

                # Take the high watermark and subtract one so the consumer will get the last msg from each partition:
                topic_partition.offset = hi - 1
                offset_assignments.append(topic_partition)

            if not offset_assignments:
                continue

            self.consumer.assign(offset_assignments)
            partition_ids_consumed = set()

            while len(partition_ids_consumed) < len(offset_assignments):
                msg = self.consumer.poll(self.kafka_timeout_seconds)

                if msg.topic() != topic_name:
                    raise Exception("Message consumed from unexpected topic")
                if msg.partition() in partition_ids_consumed:
                    raise Exception(f"Saw more than one message from same partition: topic {msg.topic()}, "
                                    f"partition {msg.partition()}")

                latest_messages_by_topic[topic_name].append(msg)
                partition_ids_consumed.add(msg.partition())

        return latest_messages_by_topic
