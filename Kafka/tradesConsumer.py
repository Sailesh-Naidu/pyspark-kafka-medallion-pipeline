from confluent_kafka import Consumer
import logging
import json

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup3',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False,
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["trades"])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        logging.error(msg.error())
        continue
    print(json.loads(msg.value().decode("utf-8")),f"partition: {msg.partition()}, offset: {msg.offset()}")
    consumer.commit()