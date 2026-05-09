import json
from confluent_kafka import Producer
import logging
from tradesProducerHelper import *
import time

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    stop_signal = False
    def wait_for_stop():
        global stop_signal
        input("Press Enter to stop producer.....\n")
        stop_signal = True

    producer_conf = {
        "bootstrap.servers": "localhost:9092",
        "acks": 'all'
    }
    producer = Producer(producer_conf)

    import threading
    threading.Thread(target=wait_for_stop, daemon=True).start()

    def delivery_report(err, msg):
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(
                f"produced to {msg.topic()} |"
                f"partition={msg.partition()} offset ={msg.offset()}"
            )

    while not stop_signal:
        time.sleep(0.01)
        trade = generate_trade()

        trade = inject_late_data(trade)
        trade = inject_duplicates(trade)
        trade = inject_bad_data(trade)
        try:
            producer.produce(
                topic='trades',
                key=str(trade.get('trader_id', 'UNKNOWN')).encode("utf-8"),
                value=json.dumps(trade).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            logger.info(f"Producing: {trade}")
        except BufferError:
            producer.poll(1)

    producer.flush(5)

if __name__ == "__main__":
    main()

