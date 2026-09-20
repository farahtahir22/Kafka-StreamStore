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
python3 iceberg_writer.py
```

Useful commands:
```bash
ice log                # view commit history for the orders table
python3 iceberg_query.py   # query the table (PyArrow + DuckDB)
```

**Note:** `pyiceberg` is pinned to `0.9.1` in `requirements.txt` — newer versions add abstract methods that `boringcatalog==0.4.0` doesn't implement yet, which breaks catalog initialization.
