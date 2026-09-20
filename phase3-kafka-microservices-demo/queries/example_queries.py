"""
Example analytical queries against the shop lakehouse tables. Pulls
each Iceberg table's data as Arrow via PyIceberg, then runs SQL over it
with DuckDB. Run this after the four services have processed a batch
of orders:
    python example_queries.py
"""

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import NAMESPACE, get_catalog

catalog = get_catalog()

orders = catalog.load_table(f"{NAMESPACE}.orders").scan().to_arrow()
invoices = catalog.load_table(f"{NAMESPACE}.invoices").scan().to_arrow()
inventory_events = catalog.load_table(f"{NAMESPACE}.inventory_events").scan().to_arrow()

con = duckdb.connect()
con.register("orders", orders)
con.register("invoices", invoices)
con.register("inventory_events", inventory_events)

print("\n=== Revenue per day ===")
print(con.execute("""
    SELECT substr(issued_at, 1, 10) AS day,
           round(sum(total), 2)      AS revenue,
           count(*)                  AS invoice_count
    FROM invoices
    GROUP BY day
    ORDER BY day
""").fetchdf())

print("\n=== Best-selling items ===")
print(con.execute("""
    SELECT item_id, item_name, sum(quantity_deducted) AS units_sold
    FROM inventory_events
    GROUP BY item_id, item_name
    ORDER BY units_sold DESC
""").fetchdf())

print("\n=== Items running low on stock (< 10 remaining) ===")
print(con.execute("""
    SELECT item_id, item_name, min(remaining_stock) AS current_stock
    FROM inventory_events
    GROUP BY item_id, item_name
    HAVING min(remaining_stock) < 10
""").fetchdf())

print("\n=== Orders and lifetime spend per user ===")
print(con.execute("""
    SELECT "user", count(*) AS order_count, round(sum(total_amount), 2) AS lifetime_spend
    FROM orders
    GROUP BY "user"
    ORDER BY lifetime_spend DESC
""").fetchdf())

# Bonus: this one is Iceberg-specific, not just SQL - every append created
# a new snapshot. Iceberg tracks that history for you automatically.
print("\n=== Snapshot history for the orders table ===")
orders_table = catalog.load_table(f"{NAMESPACE}.orders")
for snap in orders_table.metadata.snapshots:
    print(f"  snapshot {snap.snapshot_id} at {snap.timestamp_ms}  ->  {snap.summary}")
