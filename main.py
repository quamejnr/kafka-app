from typing import Dict
from kafka import KafkaProducer
import json
import time
from model import OrderEvent
from faker import Faker

fake = Faker()


def get_order() -> Dict:
    # order status being "D" depicts draft
    order = OrderEvent(user=fake.name(), sale_id=fake.random_int(), order_status="D")
    order = order.dict()
    return order


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def main():
    while True:
        order = get_order()
        print(order)
        producer.send("order_requested", order)
        time.sleep(10)


if __name__ == "__main__":
    main()
