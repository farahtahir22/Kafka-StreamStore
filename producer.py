from confluent_kafka import Producer
from uuid import uuid4
from json import dumps

producer_config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} \n Message: {msg.value().decode('utf-8')}")


order = {
    "order_id": str(uuid4()),
    "user": "ameelarara",
    "order_date": "2023-06-01",
    "items": "[{'item_id': 'lol', 'quantity': 10}]"
}

value = dumps(order).encode('utf-8')

# create a new order or appended if exists
producer.produce(topic="orders", value=value, callback=delivery_report)

# before exit, it pushes unsent events to the broker and waits for delivery reports. This is important to ensure that all messages are sent before the application exits.
# best practice to get application to work cleanly, ensures all messages are sent before exiting
producer.flush()
