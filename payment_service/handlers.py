from kafka import KafkaConsumer
import json
from faker import Faker

from producer import PayementServiceProducer

fake = Faker()

def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="payment-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    return consumer


def handle_order_requested():
    consumer = get_consumer('order_requested')
    producer = PayementServiceProducer()  
    for msg in consumer:
        order = msg.value
        order['transaction_id'] = fake.sha256()
        print("Payment Processed", order)
        producer.publish_to_payment_processed(order)

if __name__ == "__main__":
    handle_order_requested()