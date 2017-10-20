
This is a wrapper for KafkaProducer and KafkaConsumer with following
conveniences:
  * Server address
  * Single producer to share
"""
import os

from kafka import KafkaConsumer, KafkaProducer

# _KAFKA_PRODUCER = None

KAFKA_ADDR = os.environ.get("KAFKA_ADDRESS", "localhost:9092")


def get_kafka_producer(new=False):
    """
    Returns a Kafka producer. By default multiple calls return the same
    producer for efficiency.

    :param bool new: If set to True, a new instance of Kafka producer will be
        returned.
    :return: An instance of KafkaProducer
    """
    if not new:
        _PRODUCER = getattr(get_kafka_producer, '_PRODUCER', None)
        if _PRODUCER is None:
            _PRODUCER = KafkaProducer(bootstrap_servers=(KAFKA_ADDR,))
            setattr(get_kafka_producer, '_PRODUCER', _PRODUCER)
        return _PRODUCER
    else:
        return KafkaProducer(bootstrap_servers=(KAFKA_ADDR,))


def get_kafka_consumer():
    """
    Create a new Kafka consumer.

    :return:  An instance of KafkaConsumer
    """
    return KafkaConsumer(bootstrap_servers=(KAFKA_ADDR,))

