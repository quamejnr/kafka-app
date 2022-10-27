from kafka import KafkaConsumer
import json
from faker import Faker

from payment_service.producer import PaymentServiceProducer
import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

fake = Faker()


def get_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id="payment-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    topics = ["order_requested"]
    consumer.subscribe(topics)
    return consumer


def handle_order_requested(msg):
    producer = PaymentServiceProducer()
    order = msg.value
    order["transaction_id"] = fake.sha256()
    logger.info(f"Payment process completed - {order}")
    producer.publish_to_payment_processed(order)


def payment_service():
    print("Payment service listening...")
    handlers = {
        "order_requested": handle_order_requested,
    }
    consumer = get_consumer()
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    payment_service()
