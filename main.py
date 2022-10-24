from kafka import KafkaProducer
import json
import time
from model import Order
from faker import Faker

fake = Faker()

def get_order():
    order = Order(
        user = fake.name(),
        sale_id = fake.random_int(),
        status = "D"
    )
    order = order.dict()
    return order

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


if __name__ == "__main__":
    while True:
        order = get_order()
        print(order)
        producer.send("order_requested", order)
        time.sleep(10)