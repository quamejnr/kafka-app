import json
import logging
from utils import get_consumer

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

handler = logging.FileHandler("app.log")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def handle_order_confirmed(msg):
    username = msg.value["user"]
    logger.info(f"(Order confirmed) Email sent to {username}")


def handle_order_completed(msg):
    username = msg.value["user"]
    logger.info(f"(Order delivered) Email sent to {username}")


def messaging_service():
    print("Messaging service listening...")
    handlers = {
        "order_confirmed": handle_order_confirmed,
        "order_completed": handle_order_completed,
    }
    topics=["order_confirmed", "order_completed"]
    consumer = get_consumer("messaging-group", topics)
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    messaging_service()
