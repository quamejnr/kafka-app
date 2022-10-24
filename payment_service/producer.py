from kafka import KafkaProducer
import json

class PayementServiceProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        
    def publish_to_payment_processed(self, data):
        self.producer.send('payment_processed', data)