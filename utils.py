from kafka import KafkaProducer, KafkaConsumer
import json
from typing import Dict, Any, List

def produce_kafka_event(event_name: str, data: Dict[str, Any]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    producer.send(event_name, data)
    

def get_consumer(group_id: str, topics: List) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    consumer.subscribe(topics)
    return consumer