from kafka import KafkaConsumer
import json
from faker import Faker
from producer import ShipmentServiceProducer

fake = Faker()


def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="shipment-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print("Shipment service listening...")
    return consumer


def handle_order_confirmed():
    consumer = get_consumer("order_confirmed")
    producer = ShipmentServiceProducer()
    for msg in consumer:
        order = msg.value
        order["shipment_status"] = "D"
        print("Shipment Prepared", order)
        producer.publish_shipment_prepared(order)


def handle_shipment_prepared():
    consumer = get_consumer("shipment_prepared")
    producer = ShipmentServiceProducer()
    for msg in consumer:
        order = msg.value
        order["shipment_status"] = "A"
        print("Shipment dispatched", order)
        producer.publish_to_shipment_dispatched(order)


def handle_shipment_dispatched():
    consumer = get_consumer("shipment_dispatched")
    producer = ShipmentServiceProducer()
    for msg in consumer:
        order = msg.value
        order["shipment_status"] = "C"
        print("Shipment delivered", order)
        producer.publish_to_shipment_delivered(order)
