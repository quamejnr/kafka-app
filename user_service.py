from kafka import KafkaConsumer
import json


class UserService:

    users_db = []

    def __init__(self) -> None:
        self.consumer = KafkaConsumer(
            "registered_user",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            group_id="consumer-group-a",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

    def handle_registered_user(self) -> None:
        """Handle registered user event."""
        for msg in self.consumer:
            user = msg.value
            username = user["name"]
            self.users_db.append(user)
            print(f"User: {username} has been registered successfully.")


if __name__ == "__main__":
    service = UserService()
    service.handle_registered_user()
