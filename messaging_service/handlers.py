from kafka import KafkaConsumer
import json
import asyncio


def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="messaging-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print("Messaging service listening...")
    return consumer


def handle_order_confirmed():
    consumer = get_consumer("order_confirmed")
    for msg in consumer:
        username = msg.value["user"]
        sale_id = msg.value["sale_id"]
        print(f"Hello {username}, Your order: {sale_id}, has been confirmed.")


def handle_order_completed():
    consumer = get_consumer("order_completed")
    for msg in consumer:
        username = msg.value["user"]
        sale_id = msg.value["sale_id"]
        print(f"Hello {username}, Your order: {sale_id}, has been delivered.")
