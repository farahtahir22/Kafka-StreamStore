# StreamStore

A simple Kafka producer/consumer setup for practicing order event streaming.

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

## Usage

In one terminal, start the consumer:
```bash
python3 order-tracker.py
```

In another terminal, run the producer to send an order:
```bash
python3 producer.py
```
