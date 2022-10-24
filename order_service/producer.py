from kafka import KafkaProducer
import json

class OrderServiceProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        
    def publish_to_order_requested(self, data):
        self.producer.send('order_requested', data)
        
    def publish_to_order_confirmed(self, data):
        self.producer.send('order_confirmed', data)