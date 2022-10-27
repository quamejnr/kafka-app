from kafka import KafkaConsumer
import json
from shipment_service.producer import ShipmentServiceProducer
import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def get_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id="shipment-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    topics = ["order_confirmed", "shipment_prepared", "shipment_dispatched"]
    consumer.subscribe(topics)
    return consumer


def handle_order_confirmed(msg):
    producer = ShipmentServiceProducer()
    order = msg.value
    order["shipment_status"] = "D"
    logger.info(f"Shipment prepared - {order}")
    producer.publish_shipment_prepared(order)


def handle_shipment_prepared(msg):
    producer = ShipmentServiceProducer()
    order = msg.value
    order["shipment_status"] = "A"  # Change shipment status to "A" to depict "shipment activated"
    logger.info(f"Shipment dispatched - {order}")
    producer.publish_to_shipment_dispatched(order)


def handle_shipment_dispatched(msg):
    producer = ShipmentServiceProducer()
    order = msg.value
    order["shipment_status"] = "C"  # change shipment status to 'C' to depict "shipment completed"
    logger.info(f"Shipment delivered - {order}")
    producer.publish_to_shipment_delivered(order)


def shipment_service():
    print("Shipment service listening...")
    handlers = {
        "order_confirmed": handle_order_confirmed,
        "shipment_prepared": handle_shipment_prepared,
        "shipment_dispatched": handle_shipment_dispatched,
    }
    consumer = get_consumer()
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    shipment_service()
