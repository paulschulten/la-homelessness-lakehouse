# lh_orch/lh_assets/gold_311.py

import sys
from pathlib import Path

import dagster as dg
import pandas as pd
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

from lh_orch.lh_assets.silver_311 import silver_311_encampment, SILVER_PATH

ICEBERG_NAMESPACE = "gold"

DATE_COLUMNS = ["createddate", "updateddate", "servicedate", "closeddate"]
NUMERIC_COLUMNS = ["latitude", "longitude"]

# Everything else is kept as text — request/case metadata, address fields,
# and geography descriptors (council district, neighborhood council, police
# precinct). No tract-level field exists in this source; lat/long is the
# finest-grain geography available, and could support tract-level geocoding
# downstream if pursued later — not attempted at ingestion time.
TEXT_COLUMNS = [
    "srnumber", "actiontaken", "owner", "requesttype", "status", "requestsource",
    "createdbyuserorganization", "mobileos", "anonymous", "assignto",
    "addressverified", "approximateaddress", "address", "housenumber",
    "direction", "streetname", "suffix", "zipcode", "tbmpage", "tbmcolumn",
    "tbmrow", "apc", "cd", "cdmember", "nc", "ncname", "policeprecinct",
]


def _ensure_namespace(catalog):
    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)


def _get_or_create_table(catalog, table_name, schema):
    identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
    if not catalog.table_exists(identifier):
        return catalog.create_table(identifier, schema=schema)
    return catalog.load_table(identifier)


def _evolve_schema(table, arrow_schema):
    """Add any columns present in the incoming data but not yet in the
    Iceberg table's schema — same defensive pattern used for HIC, in case
    LA's 311 schema changes in a future year's dataset."""
    with table.update_schema() as update:
        update.union_by_name(arrow_schema)


@dg.asset(
    deps=[silver_311_encampment],
    group_name="encampment_311",
    description=(
        "Fact table of LA City MyLA311 homeless encampment service requests, "
        "one row per request, 2020-2024 (2025 excluded — source dataset "
        "stale, see bronze_311.py). Includes request lifecycle timestamps "
        "(created/updated/service/closed), status, address, lat/long, and "
        "council district / neighborhood council / police precinct. No "
        "tract-level field exists in the source; lat/long is the finest "
        "geography available and would need geocoding to join to "
        "gold.dim_tract — not attempted here. srnumber is LA's own unique "
        "service-request identifier, usable directly as a natural key. "
        "Landed in gold.fact_311_encampment."
    ),
)
def gold_fact_311_encampment(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string")

    ordered_columns = (
        [c for c in TEXT_COLUMNS if c in df.columns]
        + [c for c in DATE_COLUMNS if c in df.columns]
        + [c for c in NUMERIC_COLUMNS if c in df.columns]
        + ["source_year"]
    )
    fact_df = df[ordered_columns].copy()

    schema_fields = []
    for col in ordered_columns:
        if col in TEXT_COLUMNS:
            schema_fields.append((col, pa.string()))
        elif col in DATE_COLUMNS:
            schema_fields.append((col, pa.timestamp("us")))
        elif col in NUMERIC_COLUMNS:
            schema_fields.append((col, pa.float64()))
        elif col == "source_year":
            schema_fields.append((col, pa.int32()))

    schema = pa.schema(schema_fields)

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "fact_311_encampment", schema)
    _evolve_schema(table, schema)

    arrow_table = pa.Table.from_pandas(fact_df, schema=schema, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(fact_df)} rows to gold.fact_311_encampment")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(fact_df))})