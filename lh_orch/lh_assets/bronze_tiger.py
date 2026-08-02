# lh_orch/lh_assets/bronze_tiger.py

import sys
from pathlib import Path

import dagster as dg
import pyarrow as pa
from pygris import tracts

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

ICEBERG_NAMESPACE = "bronze"
ICEBERG_TABLE_NAME = "tiger_tracts"

STATE_FIPS = "06"    # California
COUNTY_FIPS = "037"  # Los Angeles County
YEAR = 2023           # match your ACS/PIT vintage

BRONZE_SCHEMA = pa.schema([
    ("tract_fips", pa.string()),
    ("tract_name", pa.string()),
    ("state_fips", pa.string()),
    ("county_fips", pa.string()),
    ("year", pa.int32()),
    ("land_area_sqm", pa.float64()),
    ("water_area_sqm", pa.float64()),
    ("internal_lat", pa.float64()),
    ("internal_lon", pa.float64()),
    ("geometry_wkt", pa.string()),
])


@dg.asset(
    name="bronze_tiger_tracts",
    group_name="geography",
    description=(
        "Census TIGER/Line tract boundaries for LA County, pulled via pygris. "
        "Geometry stored as WKT for downstream spatial checks against ACS/PIT "
        "tract coverage. Landed in bronze.tiger_tracts."
    ),
)
def bronze_tiger_tracts(context: dg.AssetExecutionContext):
    context.log.info(f"Fetching TIGER tracts for county {COUNTY_FIPS}, year {YEAR}")

    gdf = tracts(state=STATE_FIPS, county=COUNTY_FIPS, year=YEAR, cache=True)

    df = gdf.rename(columns={
        "GEOID": "tract_fips",
        "NAMELSAD": "tract_name",
        "STATEFP": "state_fips",
        "COUNTYFP": "county_fips",
        "ALAND": "land_area_sqm",
        "AWATER": "water_area_sqm",
        "INTPTLAT": "internal_lat",
        "INTPTLON": "internal_lon",
    })

    df["year"] = YEAR
    df["internal_lat"] = df["internal_lat"].astype(float)
    df["internal_lon"] = df["internal_lon"].astype(float)
    df["geometry_wkt"] = df["geometry"].apply(lambda geom: geom.wkt)

    df = df[[
        "tract_fips", "tract_name", "state_fips", "county_fips", "year",
        "land_area_sqm", "water_area_sqm", "internal_lat", "internal_lon",
        "geometry_wkt",
    ]].reset_index(drop=True)

    context.log.info(f"Fetched {len(df)} tracts")

    catalog = get_catalog()
    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)
    identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"

    if not catalog.table_exists(identifier):
        table = catalog.create_table(identifier, schema=BRONZE_SCHEMA)
    else:
        table = catalog.load_table(identifier)

    arrow_table = pa.Table.from_pandas(df, schema=BRONZE_SCHEMA, preserve_index=False)

    # Static reference data for a given vintage -> overwrite, not append,
    # so re-running doesn't duplicate rows.
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(df)} rows to bronze.tiger_tracts")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(df))})