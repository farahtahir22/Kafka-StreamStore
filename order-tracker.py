from confluent_kafka import Consumer
from json import loads

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    # tells consulmer what to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): "earliest" means automatically reset the offset to the earliest offset
    "auto.offset.reset": "earliest"
}
consumer = Consumer(consumer_config)


consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to 'orders' topic...")

try:
    while True:
        msg = consumer.poll(1.0)  # timeout of 1 second

        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = loads(value)
        print(
            f"Received order: {order['order_id']}, User: {order['user']}, Date: {order['order_date']}, Items: {order['items']}")
except KeyboardInterrupt:
    print("🛑 Consumer is shutting down...")

finally:
    consumer.close()
    print("✅ Consumer has been closed.")