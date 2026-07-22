import csv
from iceberg_catalog import get_catalog

with open("table_config.csv", newline="") as f:
    configured = {row["table_id"].strip() for row in csv.DictReader(f, delimiter="\t")}

catalog = get_catalog()
table = catalog.load_table("bronze.acs_estimates")
arrow_table = table.scan(selected_fields=["table_id"]).to_arrow()
ingested = set(arrow_table.column("table_id").to_pylist())

missing = sorted(configured - ingested)
extra = sorted(ingested - configured)

print(f"Configured: {len(configured)} tables")
print(f"Ingested:   {len(ingested)} distinct table_ids")
print("Missing (in config, never landed):", missing)
print("Extra (in Iceberg, not in config):", extra)
