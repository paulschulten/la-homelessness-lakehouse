# lh_orch/lh_assets/silver_expenses.py

import dagster as dg
import pandas as pd
from pathlib import Path
import json

# Resolve project root from this file's location
project_root = Path(__file__).resolve().parents[2]

DATA_DIR = project_root / "02_data"

BRONZE_DIR = DATA_DIR / "01_raw" / "lacity" / "01_homelessness_expenses"
SILVER_DIR = DATA_DIR / "02_silver" / "lacity" / "01_homelessness_expenses"


@dg.asset(
    name="silver_expenses",
    description="Cleaned up and internally reconciled homelessness expenses data.",
    deps=["bronze_expenses"],   # ensures correct dependency ordering
)
def silver_expenses(context: dg.AssetExecutionContext):
    context.log.info(f"BRONZE_DIR resolves to: {BRONZE_DIR.resolve()}")
    context.log.info(f"SILVER_DIR resolves to: {SILVER_DIR.resolve()}")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # Load latest Bronze file
    bronze_files = sorted(BRONZE_DIR.glob("*.json"))
    if not bronze_files:
        raise FileNotFoundError("No Bronze JSON files found.")
    latest_file = bronze_files[-1]

    context.log.info(f"Loading bronze file: {latest_file}")

    with open(latest_file, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Normalize column names
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("$", "")
        .str.replace("-", "_")
    )

    # Convert date columns
    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Convert money columns
    money_cols = [c for c in df.columns if "amount" in c or "cost" in c]
    for col in money_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("$", "")
            .str.replace(",", "")
            .astype(float)
        )

    df = df.drop_duplicates()

    # Write Silver output
    output_path = SILVER_DIR / "homelessness_expenses_silver.parquet"
    df.to_parquet(output_path, index=False)

    context.log.info(f"Silver file written to: {output_path}")

    return dg.MaterializeResult(
        metadata={
            "row_count": len(df),
            "output_path": str(output_path),
            "columns": str(list(df.columns)),
        }
    )
