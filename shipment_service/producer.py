from kafka import KafkaProducer
import json


class ShipmentServiceProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    def publish_shipment_prepared(self, data):
        self.producer.send("shipment_prepared", data)

    def publish_to_shipment_dispatched(self, data):
        self.producer.send("shipment_dispatched", data)

    def publish_to_shipment_delivered(self, data):
        self.producer.send("shipment_delivered", data)
