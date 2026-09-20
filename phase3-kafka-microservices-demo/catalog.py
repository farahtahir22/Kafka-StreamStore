"""
Shared Iceberg catalog + table schemas for the order microservices demo.
All four services (orders archive, notifications, inventory, invoices)
import this so they agree on one catalog and one set of table schemas.
"""

import os
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog

WAREHOUSE_PATH = str(Path(__file__).resolve().parent / "iceberg_warehouse")
NAMESPACE = "shop"

ORDERS_SCHEMA = pa.schema([
    ("order_id", pa.string()),
    ("user", pa.string()),
    ("order_date", pa.string()),
    ("items", pa.string()),        # JSON-encoded list of line items
    ("total_amount", pa.float64()),
])

INVENTORY_EVENTS_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("order_id", pa.string()),
    ("item_id", pa.string()),
    ("item_name", pa.string()),
    ("quantity_deducted", pa.int64()),
    ("remaining_stock", pa.int64()),
    ("event_time", pa.string()),
])

INVOICES_SCHEMA = pa.schema([
    ("invoice_id", pa.string()),
    ("order_id", pa.string()),
    ("user", pa.string()),
    ("item_count", pa.int64()),
    ("subtotal", pa.float64()),
    ("tax", pa.float64()),
    ("total", pa.float64()),
    ("issued_at", pa.string()),
])


def get_catalog():
    """One local, SQLite-backed catalog shared by every service."""
    os.makedirs(WAREHOUSE_PATH, exist_ok=True)
    return load_catalog(
        "shop_catalog",
        **{
            "type": "sql",
            "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
            "warehouse": f"file://{WAREHOUSE_PATH}",
        },
    )


def ensure_namespace(catalog):
    try:
        catalog.create_namespace(NAMESPACE)
    except Exception:
        pass  # already exists - fine


def get_or_create_table(catalog, table_name, schema):
    identifier = f"{NAMESPACE}.{table_name}"
    if catalog.table_exists(identifier):
        return catalog.load_table(identifier)
    return catalog.create_table(identifier, schema=schema)
