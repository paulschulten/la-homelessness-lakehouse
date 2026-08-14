# lh_orch/lh_assets/gold_homeless_students.py

import re
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

from lh_orch.lh_assets.silver_homeless_students import silver_homeless_students, SILVER_PATH

ICEBERG_NAMESPACE = "gold"


def _sanitize(col: str) -> str:
    """Convert a raw source column name to an Iceberg-safe snake_case field
    name, e.g. 'Hotels/Motels (percent)' -> 'hotels_motels_percent'."""
    s = col.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


COUNT_COLUMNS = [
    "Cumulative Enrollment",
    "Homeless Student Enrollment",
    "Temporarily Doubled Up",
    "Temporary Shelters",
    "Hotels/Motels",
    "Temporarily Unsheltered",
    "Missing/Unknown",
]

PERCENT_COLUMNS = [
    "Temporarily Doubled Up (percent)",
    "Temporary Shelters (percent)",
    "Hotels/Motels (percent)",
    "Temporarily Unsheltered (percent)",
    "Missing/Unknown (percent)",
]

TEXT_COLUMNS = [
    "Academic Year", "Aggregate Level", "County Code", "District Code",
    "School Code", "County Name", "District Name", "School Name",
    "Charter School", "DASS", "Reporting Category", "Entity Type",
]

ALL_RENAME = {c: _sanitize(c) for c in TEXT_COLUMNS + COUNT_COLUMNS + PERCENT_COLUMNS}


def _ensure_namespace(catalog):
    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)


def _get_or_create_table(catalog, table_name, schema):
    identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
    if not catalog.table_exists(identifier):
        return catalog.create_table(identifier, schema=schema)
    return catalog.load_table(identifier)


def _evolve_schema(table, arrow_schema):
    with table.update_schema() as update:
        update.union_by_name(arrow_schema)


@dg.asset(
    deps=[silver_homeless_students],
    group_name="homeless_students",
    description=(
        "Fact table of CDE Homeless Student Enrollment by Dwelling Type, LA "
        "County only, 2019-20 through 2024-25. One row per "
        "county/district/school x charter_school x dass x reporting_category "
        "combination, matching the source's own reporting structure. "
        "IMPORTANT — avoiding double-counting: this table contains "
        "overlapping/nested rows by design (e.g. a district's charter and "
        "non-charter schools are each broken out separately, AND rolled up "
        "into an 'All' row for that district). Summing "
        "homeless_student_enrollment across all rows for a given year will "
        "massively over-count. For a true unduplicated total (e.g. LA "
        "County-wide student count for one year), filter to "
        "aggregate_level = 'C' (county level, not district/school) AND "
        "charter_school = 'All' AND dass = 'All' AND reporting_category = "
        "'TA' (Total). Column names sanitized to snake_case from the "
        "source's original headers (e.g. 'Hotels/Motels (percent)' -> "
        "hotels_motels_percent) to satisfy Iceberg's field-name "
        "requirements. Suppressed cells (source privacy threshold, groups "
        "<= 10 students) are real null, not the source's literal '*'. "
        "entity_type (district-grain rows only) marks 'county_office' (LA "
        "County Office of Education — not a real district, overlaps with "
        "real districts' enrollment) and 'sbe_charter_school' (individual "
        "State Board of Education charter schools appearing at district "
        "grain) — filter these out for a clean geographic-district "
        "ranking/comparison; 'school_district' marks genuine districts. "
        "district_name also has one known CDE rename normalized upstream "
        "in silver ('Whittier City Elementary' -> 'Whittier City') so that "
        "district's multi-year trajectory doesn't fragment. "
        "Landed in gold.fact_homeless_students."
    ),
)
def gold_fact_homeless_students(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)

    for col in COUNT_COLUMNS + PERCENT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string")

    df = df.rename(columns=ALL_RENAME)

    schema_fields = []
    for col in TEXT_COLUMNS + COUNT_COLUMNS + PERCENT_COLUMNS:
        sanitized = ALL_RENAME[col]
        if sanitized not in df.columns:
            continue
        if col in TEXT_COLUMNS:
            schema_fields.append((sanitized, pa.string()))
        elif col in COUNT_COLUMNS:
            schema_fields.append((sanitized, pa.float64()))
        elif col in PERCENT_COLUMNS:
            schema_fields.append((sanitized, pa.float64()))

    schema = pa.schema(schema_fields)
    fact_df = df[[f[0] for f in schema_fields]].copy()

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "fact_homeless_students", schema)
    _evolve_schema(table, schema)

    arrow_table = pa.Table.from_pandas(fact_df, schema=schema, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(fact_df)} rows to gold.fact_homeless_students")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(fact_df))})