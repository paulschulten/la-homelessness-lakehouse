from dagster import asset
import pandas as pd
from pathlib import Path

@asset(
    deps=["silver_expenses"],
    description="Gold fact table for homelessness expenses, analytics-ready."
)
def gold_expenses():
    # project root: lh_orch is two levels up from this file
    project_root = Path(__file__).resolve().parents[2]

    silver_path = (
        project_root
        / "02_data/02_silver/lacity/01_homelessness_expenses/homelessness_expenses_silver.parquet"
    )

    gold_path = (
        project_root
        / "02_data/03_gold/lacity/01_homelessness_expenses/fact_homelessness_expenses.parquet"
    )

    df = pd.read_parquet(silver_path)

    # semantic renames for Gold layer
    df = df.rename(columns={
        "dept_name": "department",
        "vendor_name": "vendor",
        "fund_nm": "fund",
        "mjr_project_code": "project_code",
        "mjr_project_name": "project_name",
        "amount": "expense_amount",
        "trans_date": "transaction_date",
    })

    gold_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(gold_path, index=False)

    return len(df)
