from typing import Dict
from kafka import KafkaProducer
import json
from messaging_service.handlers import messaging_service
from model import OrderEvent
from faker import Faker
import time
import logging
import concurrent.futures
from order_service.handlers import order_service
from payment_service.handlers import payment_service
from shipment_service.handlers import shipment_service
from utils import produce_kafka_event

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

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


def request_order():
    while True:
        order = get_order()
        print(f"Incoming order - {order}")
        logger.info(f"Order requested - {order}")
        produce_kafka_event("order_requested", order)
        time.sleep(10)


def main():
    services = [messaging_service, order_service, payment_service, shipment_service, request_order]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        [executor.submit(service) for service in services]


if __name__ == "__main__":
    print("Kafka app is running...")
    main()
    
