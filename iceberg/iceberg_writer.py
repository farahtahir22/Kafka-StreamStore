"""
Consumes orders from the Kafka 'orders' topic, buffers them, and commits
each batch as a new Iceberg snapshot via the boring-catalog `ice commit` CLI.

Setup (once, in your project directory):
    pip install boringcatalog pyarrow
    ice init                     # creates catalog/catalog_boring.json + .ice/index

Then run this alongside your existing producer.py:
    python iceberg_writer.py
"""

import os
import subprocess
import tempfile
from json import loads

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

BATCH_SIZE = 5  # commit a new Iceberg snapshot every N orders
TABLE_NAME = "orders"  # lands in the default namespace: ice_default.orders

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "iceberg-writer",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

buffer = []


def commit_batch():
    if not buffer:
        return

    table = pa.Table.from_pylist(buffer)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        pq.write_table(table, tmp.name)
        tmp_path = tmp.name

    # First call creates the table (and infers its schema from the parquet
    # file); every call after that appends a new Iceberg snapshot.
    result = subprocess.run(
        ["ice", "commit", TABLE_NAME, "--source", tmp_path],
        capture_output=True,
        text=True,
    )
    os.unlink(tmp_path)

    if result.returncode != 0:
        print(f"❌ ice commit failed:\n{result.stderr}")
    else:
        print(f"📦 Committed {len(buffer)} orders as a new Iceberg snapshot")

    buffer.clear()


print("🟢 Iceberg writer running, subscribed to 'orders'...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        order = loads(msg.value().decode("utf-8"))
        buffer.append(order)
        print(f"Buffered order {order['order_id']} ({len(buffer)}/{BATCH_SIZE})")

        if len(buffer) >= BATCH_SIZE:
            commit_batch()

except KeyboardInterrupt:
    print("🛑 Shutting down, flushing remaining buffer...")
    commit_batch()

finally:
    consumer.close()
    print("✅ Consumer closed.")
