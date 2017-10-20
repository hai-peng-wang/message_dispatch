port json
import logging
import os


class KafkaLoggingHandler(logging.Handler):

    def __init__(self, producer, topic, default_extra=None):
        logging.Handler.__init__(self)
        self.topic = topic
        self.producer = producer
        self.default_extra = default_extra or {}
        self.hostname = os.uname()[1]

    def format(self, record):
        message = record.getMessage()
        extra = self.default_extra.copy()

        levels = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        severity = record.levelname
        if severity not in levels:
            # Bump all non standard serverity levels up to warning.
            severity = "WARNING"

        for key in ['category', 'component', 'code']:
            # Try to get hold of information passed when logging
            # e.g. through logging.info(..., extra={'component': 'test'})).
            value = getattr(record, key, None)
            if value is not None:
                extra[key] = value
            elif extra.get(key) is None:
                raise ValueError('Unable to log "{}" without setting the "{}" '
                                 'key.'.format(message, key))

        utcnow = datetime.datetime.utcnow()
        payload = {'version': 1,
                   'event': 'Logging',
                   'payload': {
                     'category': extra['category'],
                     'severity': severity,
                     'component': extra['component'],
                     'host': self.hostname,
                     'timestamp': utcnow.strftime('%Y-%m-%dT%H:%M:%SZ'),
                     'description': message,
                     'code': str(extra['code']),
                    },
                   }

        return json.dumps(payload)

    def emit(self, record):
        msg = self.format(record)
        self.producer.send(self.topic, msg.encode('utf-8'))
        


def example():
    from kafka import KafkaProducer
    kh = KafkaLoggingHandler(
        KafkaProducer(bootstrap_servers=("localhost:9092", )),
        "test", default_extra={'category': 'System',
                               'component': 'testing', 'code': 0})

    logger = logging.getLogger("local_logger")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(kh)
    logger.log(logging.WARNING, "Testing")
    logger.warn("Testing")
    logger.info("The %s boxing wizards jump %s", 5, "quickly")
    logger.debug("The quick brown %s jumps over the lazy %s", "fox",  "dog")
    logger.exception("This is an exception.")

    logger.info("The %s boxing wizards jump %s", 5, "quickly",
                extra={'foo': 'bar', 'component': 'Wibble'})
    try:
        import math
        math.exp(1000)
    except:
        logger.exception("Problem with %s", "math.exp")


if __name__ == '__main__':
    example()
