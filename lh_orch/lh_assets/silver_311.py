# lh_orch/lh_assets/silver_311.py

import json
from pathlib import Path

import dagster as dg
import pandas as pd

from lh_orch.lh_assets.bronze_311 import DATASET_IDS, bronze_311_encampment, raw_path_for_year

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "02_data" / "02_silver" / "lacity" / "03_311_encampment"
SILVER_PATH = SILVER_DIR / "encampment_311_silver.parquet"

# Fields that are genuinely numeric in this source.
NUMERIC_COLUMNS = ["latitude", "longitude"]

# 'location' is a nested {"type": ..., "coordinates": [...]} dict duplicating
# latitude/longitude as separate scalar fields already present in the same
# record — dropped here rather than kept as a nested/object column, which
# would risk the same parquet type-conflict issues seen with HIC's mixed-type
# columns, for no added information over lat/long.
DROP_COLUMNS = ["location"]


@dg.asset(
    deps=[bronze_311_encampment],
    group_name="encampment_311",
    description=(
        "Cleaned LA City MyLA311 homeless encampment service requests, "
        "2020-2024 (2025 excluded — source dataset stale as of ingestion, "
        "see bronze_311.py). One row per 311 request. latitude/longitude "
        "cast to float; all other fields (dates, address, council district, "
        "neighborhood council, etc.) kept as text per the project's "
        "faithful-ingestion approach. 'location' (a nested dict duplicating "
        "lat/long) is dropped as redundant. 'source_year' records which "
        "year's dataset a row was pulled from, distinct from 'createddate' "
        "(the two should agree, but are not assumed to)."
    ),
)
def silver_311_encampment(context: dg.AssetExecutionContext):
    all_years = []

    for year in DATASET_IDS.keys():
        path = raw_path_for_year(year)
        with open(path) as f:
            rows = json.load(f)

        df = pd.DataFrame(rows)

        for col in DROP_COLUMNS:
            if col in df.columns:
                df = df.drop(columns=[col])

        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Force all remaining object-dtype (text) columns to string —
        # mixed-type inference can otherwise trip up the parquet writer.
        # Replace actual NaN/empty with None first so it doesn't become the
        # literal string "nan" after the cast.
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].where(df[col].notna(), None).astype(str)
            df.loc[df[col] == "None", col] = None

        df["source_year"] = year

        context.log.info(f"{year}: loaded {len(df)} rows from {path}")
        all_years.append(df)

    combined = pd.concat(all_years, ignore_index=True, sort=False)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(SILVER_PATH, index=False)

    context.log.info(f"Silver 311 encampment file written to: {SILVER_PATH}")
    context.log.info(f"Total row count across all years: {len(combined)}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(combined)),
            "years_included": dg.MetadataValue.text(str(sorted(DATASET_IDS.keys()))),
            "output_path": dg.MetadataValue.path(str(SILVER_PATH)),
        }
    )