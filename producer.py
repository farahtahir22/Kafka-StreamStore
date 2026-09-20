from confluent_kafka import Producer
from uuid import uuid4
from json import dumps
from random import choice, randint, sample

producer_config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(producer_config)

NAMES = ["john", "emily", "michael", "sarah", "david", "jessica", "james", "laura", "daniel", "emma"]
MENU = ["burger", "pizza", "fries", "soda", "salad", "coffee", "sandwich", "pasta", "cake", "smoothie"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} \n Message: {msg.value().decode('utf-8')}")


def random_items() -> list[dict]:
    return [{"item_id": item, "quantity": randint(1, 5)} for item in sample(MENU, k=randint(1, 3))]


def build_order() -> dict:
    return {
        "order_id": str(uuid4()),
        "user": choice(NAMES),
        "order_date": "2023-06-01",
        "items": dumps(random_items()),
    }


for _ in range(10):
    value = dumps(build_order()).encode("utf-8")
    producer.produce(topic="orders", value=value, callback=delivery_report)

# ensures all messages are delivered (or their callbacks fired) before the process exits
producer.flush()
