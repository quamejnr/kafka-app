from kafka import KafkaConsumer
import json
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
        group_id="messaging-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    return consumer


def handle_order_confirmed():
    consumer = get_consumer("order_confirmed")
    for msg in consumer:
        username = msg.value["user"]
        logger.info(f"(Order confirmed) Email sent to {username}")


def handle_order_completed():
    consumer = get_consumer("order_completed")
    for msg in consumer:
        username = msg.value["user"]
        logger.info(f"(Order delivered) Email sent to {username}")


def main():
    handlers = [handle_order_confirmed, handle_order_completed]
    with ThreadPoolExecutor() as executor:
        [executor.submit(handler) for handler in handlers]


if __name__ == "__main__":
    print("Messaging service listening..")
    main()
