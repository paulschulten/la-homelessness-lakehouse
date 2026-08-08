# lh_orch/lh_assets/gold_hic.py

import hashlib
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

from lh_orch.lh_assets.silver_hic import silver_hic, SILVER_PATH

ICEBERG_NAMESPACE = "gold"


def _sanitize(col: str) -> str:
    """Convert a raw HIC column name to an Iceberg-safe snake_case field name.
    e.g. 'Beds HH w/ Children' -> 'beds_hh_w_children'
         'Additional Federal Funding?' -> 'additional_federal_funding'
         'Proj. Type' -> 'proj_type'
    """
    s = col.strip().lower()
    s = s.replace("w/o", "wo").replace("w/", "w")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# --- dim_hic_project: stable project-level attributes -----------------------
# Source has no unique project ID, so project_key is a surrogate built from
# Organization Name + Project Name (the only fields that identify a project).
DIM_PROJECT_COLUMNS = [
    "Organization Name", "Project Name", "Project ID", "Proj. Type", "Address",
    "City", "State", "SPA", "CD", "SD", "Geo Code", "Zip", "HMIS Participating",
    "Inventory Type", "Bed Type", "Target Pop.", "Victim Service Provider", "Housing Type",
]
DIM_PROJECT_RENAME = {c: _sanitize(c) for c in DIM_PROJECT_COLUMNS}

# --- fact_hic: per-project-year measures and funding/program flags ---------
NUMERIC_COLUMNS = [
    "Beds HH w/ Children", "Units HH w/ Children", "Beds HH w/o Children",
    "Beds HH w/ only Children", "Veteran Beds HH w/ Children", "Youth Beds HH w/ Children",
    "CH Beds HH w/ Children", "Veteran Beds HH w/o Children", "Youth Beds HH w/o Children",
    "CH Beds HH w/o Children", "CH Beds HH w only Children",
    "Year-Round Beds", "Total Seasonal Beds", "Overflow Beds",
    "PIT Count", "Total Beds", "Utilization Rate",
]
NUMERIC_RENAME = {c: _sanitize(c) for c in NUMERIC_COLUMNS}

DATE_COLUMNS = ["Availability Start Date", "Availability End Date"]
DATE_RENAME = {c: _sanitize(c) for c in DATE_COLUMNS}

DIM_PROJECT_SCHEMA = pa.schema(
    [("project_key", pa.string())]
    + [(DIM_PROJECT_RENAME[c], pa.string()) for c in DIM_PROJECT_COLUMNS]
)


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
    Iceberg table's schema. Source years (e.g. new funding-flag columns
    LAHSA adds/drops year to year) can introduce columns the table wasn't
    originally created with — this lets the table grow to accommodate them
    instead of failing on every new year's additions."""
    with table.update_schema() as update:
        update.union_by_name(arrow_schema)


def _project_key(org: str, project: str) -> str:
    raw = f"{org}||{project}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def _load_with_project_key() -> pd.DataFrame:
    df = pd.read_parquet(SILVER_PATH)
    df["project_key"] = df.apply(
        lambda r: _project_key(r.get("Organization Name", ""), r.get("Project Name", "")),
        axis=1,
    )
    return df


@dg.asset(
    deps=[silver_hic],
    group_name="hic",
    description=(
        "Dimension table of LAHSA HIC projects: organization, project name, project "
        "type, geography (city/spa/cd/sd/geo_code/zip/address — no tract-level field "
        "exists in this source; zip/address are only present in some years, null "
        "otherwise — 'Project ID' likewise appears only in 2022 data), "
        "inventory/bed type, target population, and housing type. project_key "
        "remains a surrogate hash of Organization Name + Project Name across all "
        "years for consistency, even in years where 'Project ID' is available; "
        "this is a deliberate choice to keep one key logic across the whole "
        "table rather than change grain/join behavior by year. "
        "Column names are sanitized to snake_case from LAHSA's original headers "
        "(e.g. 'Proj. Type' -> proj_type) since raw punctuation broke Iceberg's "
        "metadata; original headers are documented here for reference. project_key "
        "is a surrogate (hash of Organization Name + Project Name), since the "
        "source has no native project ID. One row per distinct project attribute "
        "combination seen across ingested years. NOTE: LAHSA redacts "
        "Organization Name and Project Name to 'CONFIDENTIAL' for victim-service "
        "(DV) providers to protect client safety; since project_key is derived "
        "from those two fields, confidential-provider projects are indistinguishable "
        "from one another and collapse to a shared project_key (19 such projects "
        "as of 2025 data). This is a genuine limitation of the source data, not an "
        "ingestion defect — no key derived from available fields can recover the "
        "distinction LAHSA intentionally withheld. Landed in gold.dim_hic_project."
    ),
)
def gold_dim_hic_project(context: dg.AssetExecutionContext):
    df = _load_with_project_key()

    dim_df = df[["project_key"] + DIM_PROJECT_COLUMNS].copy()
    for col in DIM_PROJECT_COLUMNS:
        dim_df[col] = dim_df[col].astype("string")

    dim_df = dim_df.rename(columns=DIM_PROJECT_RENAME)
    dim_df = dim_df.drop_duplicates().reset_index(drop=True)

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_hic_project", DIM_PROJECT_SCHEMA)
    _evolve_schema(table, DIM_PROJECT_SCHEMA)

    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_PROJECT_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_hic_project")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})


@dg.asset(
    deps=[silver_hic],
    group_name="hic",
    description=(
        "Fact table of LAHSA HIC bed/unit inventory, PIT count, and utilization "
        "rate, one row per project per year (joins to gold.dim_hic_project on "
        "project_key). Also carries all funding-source and McKinney-Vento program "
        "flags from the source as per-project-year attributes, since they can "
        "change year to year. Column names are sanitized to snake_case from "
        "LAHSA's original headers (e.g. 'PIT Count' -> pit_count) since raw "
        "punctuation broke Iceberg's metadata. No tract-level join is possible — "
        "see gold.dim_hic_project for geography grain (spa/cd/sd). NOTE: rows "
        "belonging to confidential (victim-service/DV) providers share a "
        "project_key with other confidential providers — see gold.dim_hic_project "
        "description for why. Aggregate bed/capacity totals remain accurate; "
        "project-level analysis for this subset does not resolve to distinct "
        "projects. Landed in gold.fact_hic."
    ),
)
def gold_fact_hic(context: dg.AssetExecutionContext):
    df = _load_with_project_key()

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    exclude = set(DIM_PROJECT_COLUMNS) | {"year", "Year", "project_key"}
    flag_columns = [
        c for c in df.columns
        if c not in exclude and c not in NUMERIC_COLUMNS and c not in DATE_COLUMNS
    ]
    flag_rename = {c: _sanitize(c) for c in flag_columns}
    for col in flag_columns:
        df[col] = df[col].astype("string")

    fact_columns = ["project_key", "year"] + NUMERIC_COLUMNS + DATE_COLUMNS + flag_columns
    fact_columns = [c for c in fact_columns if c in df.columns]
    fact_df = df[fact_columns].copy()

    full_rename = {**NUMERIC_RENAME, **DATE_RENAME, **flag_rename}
    fact_df = fact_df.rename(columns=full_rename)

    schema_fields = [("project_key", pa.string()), ("year", pa.int32())]
    for col in fact_columns:
        if col in ("project_key", "year"):
            continue
        elif col in NUMERIC_COLUMNS:
            schema_fields.append((full_rename[col], pa.float64()))
        elif col in DATE_COLUMNS:
            schema_fields.append((full_rename[col], pa.timestamp("us")))
        else:
            schema_fields.append((full_rename[col], pa.string()))

    schema = pa.schema(schema_fields)
    fact_df = fact_df[[f[0] for f in schema_fields]]

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "fact_hic", schema)
    _evolve_schema(table, schema)

    arrow_table = pa.Table.from_pandas(fact_df, schema=schema, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(fact_df)} rows to gold.fact_hic")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(fact_df))})