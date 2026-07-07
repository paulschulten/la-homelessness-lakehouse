from dagster import asset
import pandas as pd
from pathlib import Path
import json

# Resolve project root from this file's location
project_root = Path(__file__).resolve().parents[2]

DATA_DIR = project_root / "02_data"

BRONZE_DIR = DATA_DIR / "01_raw" / "lacity" / "01_homelessness_expenses"
SILVER_DIR = DATA_DIR / "02_silver" / "lacity" / "01_homelessness_expenses"

@asset(
    name="silver_expenses",
    description="Cleaned and normalized homelessness expenses dataset written to Silver layer.",
    deps=["bronze_expenses"]
)
def silver_expenses():
    print("DEBUG: BRONZE_DIR resolves to:", BRONZE_DIR.resolve())
    print("DEBUG: SILVER_DIR resolves to:", SILVER_DIR.resolve())

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    bronze_files = sorted(BRONZE_DIR.glob("*.json"))
    if not bronze_files:
        raise FileNotFoundError("No Bronze JSON files found.")
    latest_file = bronze_files[-1]

    with open(latest_file, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("$", "")
        .str.replace("-", "_")
    )

    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

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

    output_path = SILVER_DIR / "homelessness_expenses_silver.parquet"
    df.to_parquet(output_path, index=False)

    return str(output_path)
