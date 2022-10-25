from kafka import KafkaProducer
import json

from model import OrderEvent


class OrderServiceProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    def publish_to_order_requested(self, data: OrderEvent):
        self.producer.send("order_requested", data)

    def publish_to_order_confirmed(self, data: OrderEvent):
        self.producer.send("order_confirmed", data)

    def publish_to_order_completed(self, data: OrderEvent):
        self.producer.send("order_completed", data)
