from kafka import KafkaProducer
import json
import time

from model import get_registered_user

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


if __name__ == "__main__":
    while True:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send("registered_user", registered_user)
        time.sleep(5)
