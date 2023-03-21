from utils import produce_kafka_event, get_consumer

import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def handle_payment_processed(msg):
    order = msg.value
    order["order_status"] = "A"  # Change order status to "A" to depict order activated
    logger.info(f"Order confirmed - {order}")
    produce_kafka_event("order_confirmed", order)


def handle_shipment_delivered(msg):
    order = msg.value
    order["order_status"] = "C"  # Change order status to 'C' to depict order completed
    logger.info(f"Order completed - {order}")
    produce_kafka_event("order_completed", order)


def order_service():
    print("Order service listening...")
    handlers = {
        "payment_processed": handle_payment_processed,
        "shipment_delivered": handle_shipment_delivered,
    }
    topics=["payment_processed", "shipment_delivered"]
    consumer = get_consumer("order_group", topics)
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    order_service()
