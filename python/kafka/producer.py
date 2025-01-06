import logging
import time
from confluent_kafka import Producer


CONFIG = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000,
}


def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def send_message(producer: Producer, topic, message):
    producer.produce(topic, value=message, callback=delivery_report)
    producer.flush()


def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    producer = Producer(CONFIG)
    i = 1

    try:
        while True:
            send_message(
                producer=producer,
                topic='messages',
                message=f'Message {i}',
            )
            i += 1
            time.sleep(5)

    except KeyboardInterrupt:
        logging.info('Stopped')

if __name__ == '__main__':
    main()
