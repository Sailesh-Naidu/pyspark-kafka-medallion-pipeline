from confluent_kafka import DeserializingConsumer
import logging
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def read_schema(path):
    with open(path,"r") as f:
        schema_str = f.read()
    return schema_str

def create_consumer(avro_deserializer):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup3',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
        'value.deserializer': avro_deserializer,
    }

    return DeserializingConsumer(consumer_conf)

def create_schema_registry_client():
    schema_registry_conf = {
        "url": "http://localhost:8081",
    }
    return SchemaRegistryClient(schema_registry_conf)

def main():
    schema_registry_client = create_schema_registry_client()
    #schema_str = read_schema("../schemas/trades.avsc")
    avro_deserializer = AvroDeserializer( schema_registry_client= schema_registry_client)
    consumer = create_consumer(avro_deserializer)
    consumer.subscribe(["trades"])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(msg.error())
                continue
            print(msg.value(),f"partition: {msg.partition()}, offset: {msg.offset()}")
            consumer.commit()
    except KeyboardInterrupt as e:
        logger.error(f"Stopping consumer: {e}...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()