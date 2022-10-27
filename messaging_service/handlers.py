from kafka import KafkaConsumer
import json
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
        group_id="messaging-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    topics=["order_confirmed", "order_completed"]
    consumer.subscribe(topics)

    return consumer


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
    consumer = get_consumer()
    for msg in consumer:
        handler = handlers.get(msg.topic)
        if handler:
            handler(msg)


if __name__ == "__main__":
    messaging_service()
