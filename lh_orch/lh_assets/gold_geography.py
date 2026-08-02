# lh_orch/lh_assets/gold_geography.py

import sys
from pathlib import Path

import dagster as dg
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

from lh_orch.lh_assets.silver_tiger import silver_tiger_tracts

ICEBERG_NAMESPACE = "gold"
ICEBERG_TABLE_NAME = "dim_tiger_geography"

DIM_GEOGRAPHY_SCHEMA = pa.schema([
    ("tract_fips", pa.string()),
    ("tract_name", pa.string()),
    ("land_area_sqmi", pa.float64()),
    ("water_area_sqmi", pa.float64()),
    ("internal_lat", pa.float64()),
    ("internal_lon", pa.float64()),
    ("geometry_wkt", pa.string()),
])


@dg.asset(
    name="gold_dim_tiger_geography",
    deps=[silver_tiger_tracts],
    group_name="geography",
    description=(
        "Dimension table of LA County tract geography: centroid lat/lon, "
        "land/water area in square miles, and boundary geometry (WKT). "
        "Join to gold.fact_acs_estimates or gold.fact_pit_count on "
        "tract_fips for spatial context. Landed in gold.dim_geography."
    ),
)
def gold_dim_tiger_geography(context: dg.AssetExecutionContext):
    catalog = get_catalog()
    silver_table = catalog.load_table("silver.tiger_tracts")
    df = silver_table.scan().to_arrow().to_pandas()

    dim_df = (
        df[[
            "tract_fips", "tract_name", "land_area_sqmi", "water_area_sqmi",
            "internal_lat", "internal_lon", "geometry_wkt",
        ]]
        .drop_duplicates(subset=["tract_fips"])
        .reset_index(drop=True)
    )

    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)
    identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"

    if not catalog.table_exists(identifier):
        table = catalog.create_table(identifier, schema=DIM_GEOGRAPHY_SCHEMA)
    else:
        table = catalog.load_table(identifier)

    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_GEOGRAPHY_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_geography")
    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})