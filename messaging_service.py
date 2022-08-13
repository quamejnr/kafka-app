from kafka import KafkaConsumer
import json

class MessagingService:
    
    def __init__(self) -> None:
        self.consumer = KafkaConsumer(
            "registered_user",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            group_id="consumer-group-b",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
    
    def handle_registered_user(self) -> None:
        for msg in self.consumer:
            username = msg.value['name']
            print(f"Hello {username}, welcome to the Kafka App.")

if __name__ == '__main__':
    service = MessagingService()
    service.handle_registered_user() 
