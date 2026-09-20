"""
Produces realistic orders onto the 'orders' topic. Each order has a user
and a handful of line items pulled from a small product catalog - with
quantity and unit price - so the downstream services have real fields
to do inventory and invoicing math with (your original producer's
"items" field was a stringified list, fine for the basic demo, but not
enough for this one).

Produces a batch of orders in one run:
    python order_producer.py           # 10 orders (default)
    python order_producer.py 25        # 25 orders
"""

import random
import sys
from datetime import datetime, timezone
from json import dumps
from uuid import uuid4

from confluent_kafka import Producer

DEFAULT_ORDER_COUNT = 10

PRODUCTS = [
    {"item_id": "sku-001", "name": "Wireless Mouse", "price": 24.99},
    {"item_id": "sku-002", "name": "Mechanical Keyboard", "price": 89.99},
    {"item_id": "sku-003", "name": "USB-C Hub", "price": 34.50},
    {"item_id": "sku-004", "name": "Laptop Stand", "price": 45.00},
    {"item_id": "sku-005", "name": "Webcam 1080p", "price": 59.99},
]

USERS = ["ameela", "johndoe", "priyar", "mkim", "sofiav"]

producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Order sent to {msg.topic()} [{msg.partition()}]")


def build_order():
    chosen = random.sample(PRODUCTS, k=random.randint(1, 3))
    items = [
        {
            "item_id": p["item_id"],
            "name": p["name"],
            "quantity": random.randint(1, 4),
            "unit_price": p["price"],
        }
        for p in chosen
    ]
    return {
        "order_id": str(uuid4()),
        "user": random.choice(USERS),
        "order_date": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ORDER_COUNT

    for _ in range(count):
        order = build_order()
        value = dumps(order).encode("utf-8")
        producer.produce(topic="shop-orders", value=value, callback=delivery_report)
        print(f"Order: {order['order_id']} · {order['user']} · {len(order['items'])} item(s)")

    producer.flush()
