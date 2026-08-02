# lh_orch/lh_assets/silver_tiger.py

import sys
from pathlib import Path

import dagster as dg
import pandas as pd
import pyarrow as pa
from shapely import wkt
from shapely.validation import make_valid

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

from lh_orch.lh_assets.bronze_tiger import bronze_tiger_tracts

ICEBERG_NAMESPACE = "silver"
ICEBERG_TABLE_NAME = "tiger_tracts"

SQM_TO_SQMI = 1 / 2_589_988.11  # square meters -> square miles

SILVER_SCHEMA = pa.schema([
    ("tract_fips", pa.string()),
    ("tract_name", pa.string()),
    ("state_fips", pa.string()),
    ("county_fips", pa.string()),
    ("year", pa.int32()),
    ("land_area_sqmi", pa.float64()),
    ("water_area_sqmi", pa.float64()),
    ("internal_lat", pa.float64()),
    ("internal_lon", pa.float64()),
    ("geometry_wkt", pa.string()),
    ("geometry_valid", pa.bool_()),
    ("processed_at", pa.string()),
])


def _validate_and_fix(wkt_str):
    """Parse WKT, repair invalid geometry if needed, return (wkt, was_valid)."""
    try:
        geom = wkt.loads(wkt_str)
    except Exception:
        return None, False

    if geom.is_valid:
        return geom.wkt, True

    fixed = make_valid(geom)
    return fixed.wkt, False


@dg.asset(
    name="silver_tiger_tracts",
    deps=[bronze_tiger_tracts],
    group_name="geography",
    description=(
        "TIGER tract geometry validated and repaired (invalid polygons fixed "
        "via shapely make_valid), with area fields converted from square "
        "meters to square miles. Landed in silver.tiger_tracts."
    ),
)
def silver_tiger_tracts(context: dg.AssetExecutionContext):
    catalog = get_catalog()
    bronze_table = catalog.load_table("bronze.tiger_tracts")

    df = bronze_table.scan().to_arrow().to_pandas()
    context.log.info(f"Loaded {len(df)} rows from bronze.tiger_tracts")

    results = df["geometry_wkt"].apply(_validate_and_fix)
    df["geometry_wkt"] = results.apply(lambda r: r[0])
    df["geometry_valid"] = results.apply(lambda r: r[1])

    invalid_count = (~df["geometry_valid"]).sum()
    if invalid_count:
        context.log.info(f"Repaired {invalid_count} invalid geometries")

    df = df[df["geometry_wkt"].notna()].copy()

    df["land_area_sqmi"] = df["land_area_sqm"] * SQM_TO_SQMI
    df["water_area_sqmi"] = df["water_area_sqm"] * SQM_TO_SQMI

    df["processed_at"] = pd.Timestamp.utcnow().isoformat()

    df = df[[
        "tract_fips", "tract_name", "state_fips", "county_fips", "year",
        "land_area_sqmi", "water_area_sqmi", "internal_lat", "internal_lon",
        "geometry_wkt", "geometry_valid", "processed_at",
    ]].reset_index(drop=True)

    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)
    identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"

    if not catalog.table_exists(identifier):
        silver_table = catalog.create_table(identifier, schema=SILVER_SCHEMA)
    else:
        silver_table = catalog.load_table(identifier)

    arrow_table = pa.Table.from_pandas(df, schema=SILVER_SCHEMA, preserve_index=False)

    # Full recompute off bronze each run -> overwrite, not append.
    silver_table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(df)} rows to silver.tiger_tracts")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "repaired_geometry_count": dg.MetadataValue.int(int(invalid_count)),
        }
    )