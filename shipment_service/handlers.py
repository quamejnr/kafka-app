from utils import produce_kafka_event, get_consumer
import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def handle_order_confirmed(msg):
    order = msg.value
    order["shipment_status"] = "D"
    logger.info(f"Shipment prepared - {order}")
    produce_kafka_event("shipment_prepared", order)


def handle_shipment_prepared(msg):
    order = msg.value
    order["shipment_status"] = "A"  # Change shipment status to "A" to depict "shipment activated"
    logger.info(f"Shipment dispatched - {order}")
    produce_kafka_event("shipment_dispatched", order)


def handle_shipment_dispatched(msg):
    order = msg.value
    order["shipment_status"] = "C"  # change shipment status to 'C' to depict "shipment completed"
    logger.info(f"Shipment delivered - {order}")
    produce_kafka_event("shipment_delivered", order)


def shipment_service():
    print("Shipment service listening...")
    handlers = {
        "order_confirmed": handle_order_confirmed,
        "shipment_prepared": handle_shipment_prepared,
        "shipment_dispatched": handle_shipment_dispatched,
    }
    topics = ["order_confirmed", "shipment_prepared", "shipment_dispatched"]
    consumer = get_consumer("shipment_group", topics)
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    shipment_service()
