# StreamStore

A simple Kafka producer/consumer setup for practicing order event streaming, with an Iceberg table built on top.

## Setup

1. Start Kafka:
   ```bash
   docker compose up -d
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

## Phase 1: Producer / Consumer

In one terminal, start the consumer:
```bash
python3 order-tracker.py
```

In another terminal, run the producer to send orders:
```bash
python3 producer.py
```

## Phase 2: Iceberg

Orders can also be written into a local Iceberg table using [boring-catalog](https://pypi.org/project/boringcatalog/), a lightweight file-based Iceberg catalog. Everything lives under `.ice/` and `warehouse/` in this folder.

One-time setup:
```bash
ice init
```

Then, alongside the producer, run the Iceberg writer to consume orders and commit them as Iceberg snapshots (in batches of 5):
```bash
python3 phase2-iceberg/iceberg_writer.py
```

Useful commands:
```bash
ice log                                     # view commit history for the orders table
python3 phase2-iceberg/iceberg_query.py     # query the table (PyArrow + DuckDB)
```

**Note:** `pyiceberg` is pinned to `0.9.1` in `requirements.txt` — newer versions add abstract methods that `boringcatalog==0.4.0` doesn't implement yet, which breaks catalog initialization.

## Phase 3: Kafka Microservices Demo

A step up from Phase 2: one producer and four independent consumers reading off their own Kafka topic (`shop-orders`, separate from Phase 1/2's `orders` topic), each doing a different job off the same event stream. Uses PyIceberg's native SQL (SQLite-backed) catalog instead of boring-catalog — a separate warehouse from Phase 2's.

All commands run from `phase3-kafka-microservices-demo/`:

```bash
cd phase3-kafka-microservices-demo
```

Run each consumer alongside it, each in its own terminal:
```bash
python3 consumers/notification_service.py     # prints a simulated notification, no Iceberg write
python3 consumers/inventory_service.py         # deducts stock, logs to shop.inventory_events
python3 consumers/invoice_service.py           # builds invoices, logs to shop.invoices
python3 consumers/orders_archive_service.py    # archives raw orders to shop.orders
```

Produce a batch of orders (defaults to 10, or pass a count):
```bash
python3 order_producer.py
python3 order_producer.py 25
```

Once the consumers have processed a batch, run the example analytical queries (revenue/day, best-sellers, low-stock alert, lifetime spend per user, plus Iceberg snapshot history):
```bash
python3 queries/example_queries.py
```
