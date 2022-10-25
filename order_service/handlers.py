from kafka import KafkaConsumer
import json
from producer import OrderServiceProducer


def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="order-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print("Order service listening...")
    return consumer


def handle_payment_processed():
    consumer = get_consumer("payment_processed")
    producer = OrderServiceProducer()
    for msg in consumer:
        order = msg.value
        order["order_status"] = "A"   # Change order status to "A" to depict order activated
        print("Order Confirmed", order)
        producer.publish_to_order_confirmed(order)


def handle_shipment_delivered():
    consumer = get_consumer("shipment_delivered")
    producer = OrderServiceProducer()
    for msg in consumer:
        order = msg.value
        order["order_status"] = "C"    # Change order status to 'C' to depict order completed
        print("Order completed", order)
        producer.publish_to_order_completed(order)
