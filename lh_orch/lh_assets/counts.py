import dagster as dg
import json
import pandas as pd
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]

RAW_PATH = project_root / "02_data/01_raw/lacity/01_homelessness_expenses"
SILVER_PATH = project_root / "02_data/02_silver/lacity/01_homelessness_expenses"
GOLD_PATH = project_root / "02_data/03_gold/lacity/01_homelessness_expenses"

@dg.asset(
    description="Row count of Bronze dataset.",
)
def bronze_count(context: dg.AssetExecutionContext):
    files = sorted(RAW_PATH.glob("*.json"))
    if not files:
        context.log.warning("No raw JSON files found.")
        return dg.MaterializeResult(
            metadata={"records": dg.MetadataValue.int(0)}
        )
    with open(files[-1], "r") as f:
        data = json.load(f)
    count = len(data)
    context.log.info(f"Bronze record count: {count}")
    return dg.MaterializeResult(
        metadata={"records": dg.MetadataValue.int(count)}
    )

@dg.asset(
    deps=[bronze_count],
    description="Row count of Silver dataset.",
)
def silver_count(context: dg.AssetExecutionContext):
    files = sorted(SILVER_PATH.glob("*.parquet"))
    if not files:
        context.log.warning("No silver parquet files found.")
        return dg.MaterializeResult(
            metadata={"records": dg.MetadataValue.int(0)}
        )
    df = pd.read_parquet(files[-1])
    count = len(df)
    context.log.info(f"Silver record count: {count}")
    return dg.MaterializeResult(
        metadata={"records": dg.MetadataValue.int(count)}
    )

@dg.asset(
    deps=[silver_count],
    description="Row count of Gold dataset.",
)
def gold_count(context: dg.AssetExecutionContext):
    files = sorted(GOLD_PATH.glob("*.parquet"))
    if not files:
        context.log.warning("No gold parquet files found.")
        return dg.MaterializeResult(
            metadata={"records": dg.MetadataValue.int(0)}
        )
    df = pd.read_parquet(files[-1])
    count = len(df)
    context.log.info(f"Gold record count: {count}")
    return dg.MaterializeResult(
        metadata={"records": dg.MetadataValue.int(count)}
    )