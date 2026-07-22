from iceberg_catalog import get_catalog

catalog = get_catalog()
table = catalog.load_table("bronze.acs_estimates")
arrow_table = table.scan(selected_fields=["table_id", "variable", "tract_fips", "year"]).to_arrow()

total_rows = arrow_table.num_rows
distinct_rows = arrow_table.group_by(["table_id", "variable", "tract_fips", "year"]).aggregate([]).num_rows

print(f"Total rows: {total_rows}")
print(f"Distinct key combos: {distinct_rows}")
print(f"Duplicate rows: {total_rows - distinct_rows}")
