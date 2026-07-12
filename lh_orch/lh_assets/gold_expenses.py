# lh_orch/lh_assets/gold_expenses.py

import dagster as dg
import pandas as pd
from pathlib import Path

# Resolve project root
project_root = Path(__file__).resolve().parents[2]

SILVER_PATH = (
    project_root
    / "02_data"
    / "02_silver"
    / "lacity"
    / "01_homelessness_expenses"
    / "homelessness_expenses_silver.parquet"
)

GOLD_PATH = (
    project_root
    / "02_data"
    / "03_gold"
    / "lacity"
    / "01_homelessness_expenses"
    / "fact_homelessness_expenses.parquet"
)

@dg.asset(
    deps=["silver_expenses"],
    description="Gold fact table for homelessness expenses, analytics-ready."
)
def gold_expenses(context: dg.AssetExecutionContext):
    context.log.info(f"Reading silver parquet from: {SILVER_PATH}")

    df = pd.read_parquet(SILVER_PATH)

    # Semantic renames for Gold layer
    df = df.rename(columns={
        "dept_name": "department",
        "vendor_name": "vendor",
        "fund_nm": "fund",
        "mjr_project_code": "project_code",
        "mjr_project_name": "project_name",
        "amount": "expense_amount",
        "trans_date": "transaction_date",
    })

    GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(GOLD_PATH, index=False)

    context.log.info(f"Gold file written to: {GOLD_PATH}")
    context.log.info(f"Row count: {len(df)}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "silver_path": dg.MetadataValue.path(str(SILVER_PATH)),
            "gold_path": dg.MetadataValue.path(str(GOLD_PATH)),
            "columns": dg.MetadataValue.text(str(list(df.columns))),
        }
    )
