from kafka import KafkaConsumer
import json
from producer import OrderServiceProducer

def get_consumer(topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="order-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    return consumer

def handle_payment_processed():
    consumer = get_consumer('payment_processed')
    producer = OrderServiceProducer()   
    for msg in consumer:
        order = msg.value
        order['status'] = 'A'
        print("Order Confirmed", order)
        producer.publish_to_order_confirmed(order)

if __name__ == '__main__':
    handle_payment_processed()
    