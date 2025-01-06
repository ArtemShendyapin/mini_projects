from confluent_kafka import Consumer, KafkaException


CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Список серверов Kafka
    'group.id': 'mygroup',                  # Идентификатор группы потребителей
    'auto.offset.reset': 'earliest'         # Начальная точка чтения ('earliest' или 'latest')
}


def main():
    consumer = Consumer(CONFIG)
    consumer.subscribe(['messages'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0) 
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


if __name__ == '__main__':
    main()
