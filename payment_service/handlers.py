from faker import Faker

from utils import produce_kafka_event, get_consumer
import logging

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

fake = Faker()

def handle_order_requested(msg):
    order = msg.value
    order["transaction_id"] = fake.sha256()
    logger.info(f"Payment process completed - {order}")
    produce_kafka_event("payment_processed", order)
    
def payment_service():
    print("Payment service listening...")
    handlers = {
        "order_requested": handle_order_requested,
    }
    topics = ["order_requested"]
    consumer = get_consumer("payment_group", topics)
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    payment_service()
