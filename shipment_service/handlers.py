from kafka import KafkaConsumer
import json
from faker import Faker
from producer import ShipmentServiceProducer
from concurrent.futures import ThreadPoolExecutor
import logging

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

handler = logging.FileHandler("app.log")        
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id="shipment-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    return consumer


def handle_order_confirmed():
    consumer = get_consumer("order_confirmed")
    producer = ShipmentServiceProducer()
    for msg in consumer:
        order = msg.value
        order["shipment_status"] = "D"
        logger.info(f"Shipment prepared - {order}")
        producer.publish_shipment_prepared(order)


def handle_shipment_prepared():
    consumer = get_consumer("shipment_prepared")
    producer = ShipmentServiceProducer()
    for msg in consumer:
        order = msg.value
        order[
            "shipment_status"
        ] = "A"  # Change shipment status to "A" to depict "shipment activated"
        logger.info(f"Shipment dispatched - {order}")
        producer.publish_to_shipment_dispatched(order)


def handle_shipment_dispatched():
    consumer = get_consumer("shipment_dispatched")
    producer = ShipmentServiceProducer()
    for msg in consumer:
        order = msg.value
        order[
            "shipment_status"
        ] = "C"  # change shipment status to 'C' to depict "shipment completed"
        logger.info(f"Shipment delivered - {order}")
        producer.publish_to_shipment_delivered(order)


def main():
    handlers = [
        handle_order_confirmed,
        handle_shipment_prepared,
        handle_shipment_dispatched,
    ]
    with ThreadPoolExecutor() as executor:
        [executor.submit(handler) for handler in handlers]


if __name__ == "__main__":
    print("Shipment service listening...")
    main()
