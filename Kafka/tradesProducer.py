import logging
import json
from confluent_kafka.error import ValueSerializationError

from tradesProducerHelper import *
import time
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer


logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def read_schema(path):
    with open(path,"r") as f:
        schema_str = f.read()
    return schema_str

def create_schema_registry_client():
    schema_registry_conf = {
        "url": "http://localhost:8081",
    }
    return SchemaRegistryClient(schema_registry_conf)


def create_producer(avro_serializer):
    producer_conf = {
        "bootstrap.servers": "localhost:9092",
        "acks": 'all',
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer
    }
    return SerializingProducer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(
            f"produced to {msg.topic()} |"
            f"partition={msg.partition()} offset ={msg.offset()}"
        )

def write_to_dlq(dlq_message,path):
    with open(path,"a") as f:
        f.write(json.dumps(dlq_message) + '\n')

def main():
    schemaregistryclient = create_schema_registry_client()
    schema_str = read_schema("../schemas/trades.avsc")
    avro_serializer = AvroSerializer(schema_str=schema_str, schema_registry_client=schemaregistryclient)
    producer = create_producer(avro_serializer)
    try:
        while True:
            time.sleep(0.01)
            trade = generate_trade()

            trade = inject_late_data(trade)
            trade = inject_duplicates(trade)
            trade = inject_bad_data(trade)
            try:
                producer.produce(
                    topic='trades',
                    key=str(trade.get('trader_id', 'UNKNOWN')),
                    value=trade,
                    on_delivery=delivery_report
                )
                producer.poll(0.1)
                logger.info(f"Producing: {trade}")
            except BufferError:
                producer.poll(1)

            except ValueSerializationError as e:
                logger.error(f"Value serializer error: {e}")
                dlq_message = {
                    "error_message": str(e),
                    "failed_record": trade,
                    "failed_at": datetime.utcnow().isoformat()
                }
                write_to_dlq(dlq_message,"../Kafka/dlq_messages.json")
                producer.poll(1)
    except KeyboardInterrupt:
        logger.info(f"Closing producer.....")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()

