# lh_orch/lh_assets/silver_pit.py

from pathlib import Path

import dagster as dg
import pandas as pd

from lh_orch.lh_assets.bronze_pit import bronze_pit_count, raw_path_for_year

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "02_data" / "02_silver" / "lahsa" / "01_pit_count"
SILVER_PATH = SILVER_DIR / "pit_count_silver.parquet"

YEAR_CONFIG = {
    2020: {"sheet": "Counts_by_Tract", "tract_col": "tract"},
    2022: {"sheet": "Counts_by_TractSplit", "tract_col": "tract_split"},
    2023: {"sheet": "Counts", "tract_col": "tract_split"},
    2024: {"sheet": "Counts", "tract_col": "tract_split"},
    2025: {"sheet": "Counts", "tract_col": "Tract_Split"},
    2026: {"sheet": "Counts", "tract_col": "Tract_Split"},
}

ENCAMP_ALIAS_COLUMNS = ["totEncamp", "totMSS"]

@dg.asset(
    deps=[bronze_pit_count],
    group_name="pit_count",
    description=(
        "Cleaned PIT Count data for 2020-2026 (2021 not conducted): tract FIPS "
        "normalized to match ACS format across all years' differing tract-key "
        "column names, counts cast to int. Sub-tract split rows are preserved "
        "with their parent tract FIPS (suffix stripped) for later aggregation."
    ),
)
def silver_pit_count(context: dg.AssetExecutionContext):
    all_years = []

    for year, cfg in YEAR_CONFIG.items():
        path = raw_path_for_year(year)
        df = pd.read_excel(path, sheet_name=cfg["sheet"])

        tract_col = cfg["tract_col"]
        df[tract_col] = df[tract_col].astype(str)
        df["tract_fips"] = "06037" + df[tract_col].str.split("_").str[0].str.zfill(6)

        for geo_col in ["City", "LACity", "Community_Name", "SPA", "sd", "cd", "SD", "CD", "ca_ssd", "ca_sad", "us_cd"]:
            if geo_col in df.columns:
                df[geo_col] = df[geo_col].astype(str)

        df = df.dropna(subset=["Year"])
        df["Year"] = df["Year"].astype(int)

        if "totMSS" in df.columns and "totEncamp" not in df.columns:
            df["totEncamp"] = df["totMSS"]

        count_cols = [c for c in df.columns if c.startswith("tot") or c.startswith("Fam") or c.startswith("Ind") or c in ("ShelterCountAny", "StreetCountAny")]
        for col in count_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        context.log.info(f"{year}: parsed {len(df)} rows from sheet '{cfg['sheet']}'")
        all_years.append(df)

    combined = pd.concat(all_years, ignore_index=True, sort=False)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(SILVER_PATH, index=False)

    context.log.info(f"Silver PIT count file written to: {SILVER_PATH}")
    context.log.info(f"Total row count across all years: {len(combined)}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(combined)),
            "years_included": dg.MetadataValue.text(str(sorted(YEAR_CONFIG.keys()))),
            "output_path": dg.MetadataValue.path(str(SILVER_PATH)),
        }
    )