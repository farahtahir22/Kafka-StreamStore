"""
Consumer 1 of 4 off the 'orders' topic - simulates sending the customer
a notification. No Iceberg write here on purpose: this service's whole
job is the side effect (the notification itself), not persisted state -
not every consumer needs to write to the lakehouse.

Run alongside the other three services, each as its own process:
    python notification_service.py
"""

from json import loads

from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "notification-service",   # own group = own copy of every order
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["shop-orders"])

print("🟢 Notification service running, subscribed to 'shop-orders'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        order = loads(msg.value().decode("utf-8"))
        total = sum(i["quantity"] * i["unit_price"] for i in order["items"])
        print(
            f"📧 Notify {order['user']}: order {order['order_id']} received, "
            f"total ${total:.2f}"
        )
except KeyboardInterrupt:
    print("🛑 Shutting down...")
finally:
    consumer.close()
    print("✅ Consumer closed.")
