"""
Consumer 4 of 4 off the 'orders' topic - archives every raw order into
the shop.orders Iceberg table. This is the "orders table" you asked
for, alongside inventory and invoices. This is the PyIceberg-native
successor to the boring-catalog version from the basic demo.

Run alongside the other three services, each as its own process:
    python orders_archive_service.py
"""

import sys
from json import dumps, loads
from pathlib import Path

import pyarrow as pa
from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import ORDERS_SCHEMA, ensure_namespace, get_catalog, get_or_create_table

catalog = get_catalog()
ensure_namespace(catalog)
table = get_or_create_table(catalog, "orders", ORDERS_SCHEMA)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-archive-service",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["shop-orders"])

print("🟢 Orders archive service running, subscribed to 'shop-orders'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        order = loads(msg.value().decode("utf-8"))
        total_amount = sum(i["quantity"] * i["unit_price"] for i in order["items"])

        row = {
            "order_id": order["order_id"],
            "user": order["user"],
            "order_date": order["order_date"],
            "items": dumps(order["items"]),
            "total_amount": round(total_amount, 2),
        }

        table.append(pa.Table.from_pylist([row], schema=ORDERS_SCHEMA))
        print(f"🗄️  Archived order {order['order_id']}")

except KeyboardInterrupt:
    print("🛑 Shutting down...")
finally:
    consumer.close()
    print("✅ Consumer closed.")
