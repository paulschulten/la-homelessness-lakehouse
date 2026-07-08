# lh_orch/lh_assets/counts.py

import json
import glob
import pandas as pd
from dagster import asset, Output, MetadataValue

RAW_PATH = "02_data/01_raw/lacity/01_homelessness_expenses"
SILVER_PATH = "02_data/02_silver/lacity/01_homelessness_expenses"
GOLD_PATH = "02_data/03_gold/lacity/01_homelessness_expenses"


@asset
def raw_count():
    files = sorted(glob.glob(f"{RAW_PATH}/*.json"))
    if not files:
        return Output(0, metadata={"records": MetadataValue.int(0)})

    with open(files[-1], "r") as f:
        data = json.load(f)

    count = len(data)
    return Output(count, metadata={"records": MetadataValue.int(count)})


@asset
def silver_count():
    files = sorted(glob.glob(f"{SILVER_PATH}/*.parquet"))
    if not files:
        return Output(0, metadata={"records": MetadataValue.int(0)})

    df = pd.read_parquet(files[-1])
    count = len(df)
    return Output(count, metadata={"records": MetadataValue.int(count)})


@asset
def gold_count():
    files = sorted(glob.glob(f"{GOLD_PATH}/*.parquet"))
    if not files:
        return Output(0, metadata={"records": MetadataValue.int(0)})

    df = pd.read_parquet(files[-1])
    count = len(df)
    return Output(count, metadata={"records": MetadataValue.int(count)})
