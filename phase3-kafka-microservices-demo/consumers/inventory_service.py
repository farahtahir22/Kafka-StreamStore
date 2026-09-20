"""
Consumer 2 of 4 off the 'orders' topic - deducts ordered quantities from
a local inventory store (a plain JSON file standing in for a real
inventory DB), and logs each deduction as a row in the
shop.inventory_events Iceberg table.

Run alongside the other three services, each as its own process:
    python inventory_service.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from json import loads
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import INVENTORY_EVENTS_SCHEMA, ensure_namespace, get_catalog, get_or_create_table

INVENTORY_FILE = "inventory_store.json"
DEFAULT_STOCK = {
    "sku-001": 50, "sku-002": 30, "sku-003": 40, "sku-004": 25, "sku-005": 35,
}


def load_stock():
    if not os.path.exists(INVENTORY_FILE):
        with open(INVENTORY_FILE, "w") as f:
            json.dump(DEFAULT_STOCK, f)
    with open(INVENTORY_FILE) as f:
        return json.load(f)


def save_stock(stock):
    with open(INVENTORY_FILE, "w") as f:
        json.dump(stock, f)


catalog = get_catalog()
ensure_namespace(catalog)
table = get_or_create_table(catalog, "inventory_events", INVENTORY_EVENTS_SCHEMA)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "inventory-service",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["shop-orders"])

print("🟢 Inventory service running, subscribed to 'shop-orders'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        order = loads(msg.value().decode("utf-8"))
        stock = load_stock()
        events = []

        for item in order["items"]:
            item_id = item["item_id"]
            stock[item_id] = stock.get(item_id, 0) - item["quantity"]
            events.append({
                "event_id": str(uuid4()),
                "order_id": order["order_id"],
                "item_id": item_id,
                "item_name": item["name"],
                "quantity_deducted": item["quantity"],
                "remaining_stock": stock[item_id],
                "event_time": datetime.now(timezone.utc).isoformat(),
            })

        save_stock(stock)
        table.append(pa.Table.from_pylist(events, schema=INVENTORY_EVENTS_SCHEMA))
        print(f"📦 Updated inventory for order {order['order_id']} ({len(events)} item(s))")

except KeyboardInterrupt:
    print("🛑 Shutting down...")
finally:
    consumer.close()
    print("✅ Consumer closed.")
