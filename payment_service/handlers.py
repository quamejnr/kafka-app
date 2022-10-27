from kafka import KafkaConsumer
import json
from faker import Faker

from producer import PaymentServiceProducer
import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

fake = Faker()


def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id="payment-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    return consumer


def handle_order_requested():
    consumer = get_consumer("order_requested")
    producer = PaymentServiceProducer()
    for msg in consumer:
        order = msg.value
        order["transaction_id"] = fake.sha256()
        logger.info(f"Payment process completed - {order}")
        producer.publish_to_payment_processed(order)


if __name__ == "__main__":
    print("Payment service listening...")
    handle_order_requested()
