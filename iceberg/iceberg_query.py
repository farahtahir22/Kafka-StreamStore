from boringcatalog.catalog import BoringCatalog

TABLE_NAME = "ice_default.orders"

catalog = BoringCatalog()
table = catalog.load_table(TABLE_NAME)

scan = table.scan()

print(scan.to_arrow())
print()

con = scan.to_duckdb(table_name="orders")
print(con.sql("SELECT user, count(*) AS orders FROM orders GROUP BY user ORDER BY orders DESC").fetchall())
