from kafka import KafkaConsumer
import json
from order_service.producer import OrderServiceProducer
from concurrent.futures import ThreadPoolExecutor

import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def get_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id="order-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    topics=["payment_processed", "shipment_delivered"]
    consumer.subscribe(topics)
    return consumer


def handle_payment_processed(msg):
    producer = OrderServiceProducer()
    order = msg.value
    order["order_status"] = "A"  # Change order status to "A" to depict order activated
    logger.info(f"Order confirmed - {order}")
    producer.publish_to_order_confirmed(order)


def handle_shipment_delivered(msg):
    producer = OrderServiceProducer()
    order = msg.value
    order["order_status"] = "C"  # Change order status to 'C' to depict order completed
    logger.info(f"Order completed - {order}")
    producer.publish_to_order_completed(order)


def order_service():
    print("Order service listening...")
    handlers = {
        "payment_processed": handle_payment_processed,
        "shipment_delivered": handle_shipment_delivered,
    }
    consumer = get_consumer()
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    order_service()
