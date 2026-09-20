"""
Consumer 3 of 4 off the 'orders' topic - builds an invoice (subtotal,
tax, total) for each order and writes it to the shop.invoices Iceberg
table.

Run alongside the other three services, each as its own process:
    python invoice_service.py
"""

import sys
from datetime import datetime, timezone
from json import loads
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import INVOICES_SCHEMA, ensure_namespace, get_catalog, get_or_create_table

TAX_RATE = 0.10

catalog = get_catalog()
ensure_namespace(catalog)
table = get_or_create_table(catalog, "invoices", INVOICES_SCHEMA)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "invoice-service",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["shop-orders"])

print("🟢 Invoice service running, subscribed to 'shop-orders'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        order = loads(msg.value().decode("utf-8"))
        subtotal = sum(i["quantity"] * i["unit_price"] for i in order["items"])
        tax = round(subtotal * TAX_RATE, 2)
        total = round(subtotal + tax, 2)

        invoice = {
            "invoice_id": str(uuid4()),
            "order_id": order["order_id"],
            "user": order["user"],
            "item_count": sum(i["quantity"] for i in order["items"]),
            "subtotal": round(subtotal, 2),
            "tax": tax,
            "total": total,
            "issued_at": datetime.now(timezone.utc).isoformat(),
        }

        table.append(pa.Table.from_pylist([invoice], schema=INVOICES_SCHEMA))
        print(f"🧾 Invoice {invoice['invoice_id']} for order {order['order_id']}: ${total}")

except KeyboardInterrupt:
    print("🛑 Shutting down...")
finally:
    consumer.close()
    print("✅ Consumer closed.")
