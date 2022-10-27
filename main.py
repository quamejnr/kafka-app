from typing import Dict
from kafka import KafkaProducer
import json
from model import OrderEvent
from faker import Faker
import utils.logger as logger
import time
import logging

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

handler = logging.FileHandler("app.log")        
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

fake = Faker()


def get_order() -> Dict:
    # order status being "D" depicts draft
    order = OrderEvent(user=fake.name(), sale_id=fake.random_int(), order_status="D")
    order = order.dict()
    return order


producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def request_order():
    while True:
        order = get_order()
        print(f"Incoming order - {order}")
        logger.info(f"Order requested - {order}")
        producer.send("order_requested", order)
        time.sleep(10)


if __name__ == "__main__":
    print("Kafka app is running...")
    request_order()
