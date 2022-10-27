from kafka import KafkaConsumer
import json
from producer import OrderServiceProducer
from concurrent.futures import ThreadPoolExecutor

import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id="order-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    return consumer


def handle_payment_processed():
    consumer = get_consumer("payment_processed")
    producer = OrderServiceProducer()
    for msg in consumer:
        order = msg.value
        order[
            "order_status"
        ] = "A"  # Change order status to "A" to depict order activated
        logger.info(f"Order confirmed - {order}")
        producer.publish_to_order_confirmed(order)


def handle_shipment_delivered():
    consumer = get_consumer("shipment_delivered")
    producer = OrderServiceProducer()
    for msg in consumer:
        order = msg.value
        order[
            "order_status"
        ] = "C"  # Change order status to 'C' to depict order completed
        logger.info(f"Order completed - {order}")
        producer.publish_to_order_completed(order)


def main():
    handlers = [handle_payment_processed, handle_shipment_delivered]
    with ThreadPoolExecutor() as executor:
        [executor.submit(handler) for handler in handlers]


if __name__ == "__main__":
    print("Order service listening...")
    main()
