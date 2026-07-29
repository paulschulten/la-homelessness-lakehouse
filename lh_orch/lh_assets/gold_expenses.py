# lh_orch/lh_assets/gold_expenses.py

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

SILVER_PATH = (
    PROJECT_ROOT
    / "02_data"
    / "02_silver"
    / "lacity"
    / "01_homelessness_expenses"
    / "homelessness_expenses_silver.parquet"
)

ICEBERG_NAMESPACE = "gold"


def _ensure_namespace(catalog):
    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)


def _get_or_create_table(catalog, table_name, schema):
    identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
    if not catalog.table_exists(identifier):
        return catalog.create_table(identifier, schema=schema)
    return catalog.load_table(identifier)


FACT_SCHEMA = pa.schema([
    (":id", pa.string()),
    (":version", pa.string()),
    (":created_at", pa.string()),
    (":updated_at", pa.string()),
    ("fiscal_year", pa.string()),
    ("dept_nm", pa.string()),
    ("transaction_date", pa.string()),
    ("pstng_am", pa.float64()),
    ("vendor", pa.string()),
    ("work_order_nm", pa.string()),
    ("payment_description", pa.string()),
    ("appr_nm", pa.string()),
    ("fund", pa.string()),
    ("project_code", pa.string()),
    ("project_name", pa.string()),
    ("business_type", pa.string()),
    ("payment_type", pa.string()),
    ("work_order", pa.string()),
    ("doc_id", pa.string()),
    ("doc_cd", pa.string()),
    ("dept_cd", pa.string()),
    ("appr_cd", pa.string()),
    ("doc_actg_ln_no", pa.string()),
    ("fund_cd", pa.string()),
    ("transaction_closed", pa.string()),
    ("vend_cust_cd", pa.string()),
])

DIM_DEPARTMENT_SCHEMA = pa.schema([
    ("dept_cd", pa.string()),
    ("dept_nm", pa.string()),
])

DIM_FUND_SCHEMA = pa.schema([
    ("fund_cd", pa.string()),
    ("fund", pa.string()),
])

DIM_VENDOR_SCHEMA = pa.schema([
    ("vend_cust_cd", pa.string()),
    ("vendor", pa.string()),
])

DIM_PROJECT_SCHEMA = pa.schema([
    ("project_code", pa.string()),
    ("project_name", pa.string()),
])

@dg.asset(
    deps=["silver_expenses"],
    group_name="expenses",
    description="Fact table for homelessness expenses. Landed in gold.fact_homelessness_expenses.",
)
def gold_fact_expenses(context: dg.AssetExecutionContext):
    context.log.info(f"Reading silver parquet from: {SILVER_PATH}")
    df = pd.read_parquet(SILVER_PATH)

    df = df.rename(columns={
        "vendor_name": "vendor",
        "fund_nm": "fund",
        "mjr_project_code": "project_code",
        "mjr_project_name": "project_name",
    })

    for col in df.columns:
        if col != "pstng_am":
            df[col] = df[col].astype(str)

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "fact_homelessness_expenses", FACT_SCHEMA)

    arrow_table = pa.Table.from_pandas(df, schema=FACT_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(df)} rows to gold.fact_homelessness_expenses")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "silver_path": dg.MetadataValue.path(str(SILVER_PATH)),
        }
    )

@dg.asset(
    deps=["silver_expenses"],
    group_name="expenses",
    description="Dimension table of distinct LA City departments: code and name. Landed in gold.dim_department.",
)
def gold_dim_department(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)

    dim_df = (
        df[["dept_cd", "dept_nm"]]
        .astype(str)
        .drop_duplicates(subset=["dept_cd"])
        .reset_index(drop=True)
    )

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_department", DIM_DEPARTMENT_SCHEMA)

    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_DEPARTMENT_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_department")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})

    
@dg.asset(
    deps=["silver_expenses"],
    group_name="expenses",
    description="Dimension table of distinct LA City funds: code and name. Landed in gold.dim_fund.",
)
def gold_dim_fund(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)
    df = df.rename(columns={"fund_nm": "fund"})

    dim_df = (
        df[["fund_cd", "fund"]]
        .astype(str)
        .drop_duplicates(subset=["fund_cd"])
        .reset_index(drop=True)
    )

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_fund", DIM_FUND_SCHEMA)

    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_FUND_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_fund")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})

@dg.asset(
    deps=["silver_expenses"],
    group_name="expenses",
    description="Dimension table of distinct LA City vendors: customer code and name. Landed in gold.dim_vendor.",
)
def gold_dim_vendor(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)
    df = df.rename(columns={"vendor_name": "vendor"})

    dim_df = (
        df[["vend_cust_cd", "vendor"]]
        .astype(str)
        .drop_duplicates(subset=["vend_cust_cd"])
        .reset_index(drop=True)
    )

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_vendor", DIM_VENDOR_SCHEMA)

    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_VENDOR_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_vendor")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})

@dg.asset(
    deps=["silver_expenses"],
    group_name="expenses",
    description="Dimension table of distinct LA City major projects: code and name. Landed in gold.dim_project.",
)
def gold_dim_project(context: dg.AssetExecutionContext):
    df = pd.read_parquet(SILVER_PATH)
    df = df.rename(columns={
        "mjr_project_code": "project_code",
        "mjr_project_name": "project_name",
    })

    dim_df = (
        df[["project_code", "project_name"]]
        .astype(str)
        .drop_duplicates(subset=["project_code"])
        .reset_index(drop=True)
    )

    catalog = get_catalog()
    _ensure_namespace(catalog)
    table = _get_or_create_table(catalog, "dim_project", DIM_PROJECT_SCHEMA)

    arrow_table = pa.Table.from_pandas(dim_df, schema=DIM_PROJECT_SCHEMA, preserve_index=False)
    table.overwrite(arrow_table)

    context.log.info(f"Wrote {len(dim_df)} rows to gold.dim_project")

    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(len(dim_df))})
