"""
Shared Iceberg catalog + table schemas for the order microservices demo.
All four services (orders archive, notifications, inventory, invoices)
import this so they agree on one catalog and one set of table schemas.
"""

import os
from pathlib import Path

import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Where the SQLite catalog.db lives (table name -> storage location pointers).
# This is just local bookkeeping - the actual Parquet/metadata files below
# live in S3_WAREHOUSE (MinIO for now, real S3 later).
CATALOG_DB_DIR = str(Path(__file__).resolve().parent / "iceberg_warehouse")
NAMESPACE = "shop"

S3_WAREHOUSE = "s3://warehouse"
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9002")
S3_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
S3_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

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
    """SQLite catalog shared by every service - table data lives in S3 (MinIO locally)."""
    os.makedirs(CATALOG_DB_DIR, exist_ok=True)
    return load_catalog(
        "shop_catalog",
        **{
            "type": "sql",
            "uri": f"sqlite:///{CATALOG_DB_DIR}/catalog.db",
            "warehouse": S3_WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY_ID,
            "s3.secret-access-key": S3_SECRET_ACCESS_KEY,
            "s3.path-style-access": "true",
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
