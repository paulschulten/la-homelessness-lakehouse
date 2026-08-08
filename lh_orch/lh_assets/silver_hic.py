# lh_orch/lh_assets/silver_hic.py

from pathlib import Path

import dagster as dg
import pandas as pd

from lh_orch.lh_assets.bronze_hic import bronze_hic, raw_path_for_year

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "02_data" / "02_silver" / "lahsa" / "02_hic"
SILVER_PATH = SILVER_DIR / "hic_silver.parquet"

# Sheet name embeds the year (e.g. "2025 HIC - All Projects"), so this maps
# year -> sheet name rather than assuming a fixed sheet across years. Only
# years present here get processed; matches the years currently uncommented
# in bronze_hic.HIC_URLS. Add entries as more years are pulled in.
YEAR_CONFIG = {
    2020: {"sheet": "2020 HIC - All Projects"},
    2021: {"sheet": "2021 HIC- All Projects"},
    2022: {"sheet": "2022 HIC - All Projects"},
    2023: {"sheet": "2023 HIC - All Projects"},
    2024: {"sheet": "2024 HIC - All Projects"},
    2025: {"sheet": "2025 HIC - All Projects"},
}

GEO_COLUMNS = ["City", "SPA", "CD", "SD", "Geo Code", "Zip", "Address", "Community"]

# Columns that are junk/formatting artifacts in the source Excel, not real
# LAHSA data — safe to drop, unlike dropping an actual source column.
# 'Row #' (2022+ years) is a spreadsheet row-index artifact, not HIC data.
DROP_COLUMNS = ["Unnamed: 78", "Row #"]

# LAHSA is inconsistent with punctuation/formatting on these column names
# across years — same field, different spelling. Normalize to one canonical
# name per field so years land under the same column instead of splitting.
# Add more entries here if future years introduce similar drift.
COLUMN_RENAME = {
    "CH Beds HH w/ only Children": "CH Beds HH w only Children",
    "HMIS-Participating": "HMIS Participating",
    "ZIP": "Zip",
    "McKinney- Vento: YHDP": "McKinney- Vento: Yhdp",
}

# Some years parse these as real datetime values in Excel, others leave them
# as blank/text — normalize to string explicitly before the general object-
# dtype cast, so concat across years doesn't mix Timestamp objects with
# strings in the same column (which breaks the parquet writer).
DATE_COLUMNS = ["Availability Start Date", "Availability End Date"]


@dg.asset(
    deps=[bronze_hic],
    group_name="hic",
    description=(
        "Cleaned LAHSA Housing Inventory Count (HIC) data, project/site-level. "
        "Geography fields (City/SPA/CD/SD/Geo Code) normalized to string; all "
        "other source columns preserved as-is per the project's faithful-"
        "ingestion approach. No tract-level geography exists in this source. "
        "One column name is normalized across years to correct LAHSA's own "
        "inconsistent punctuation (see COLUMN_RENAME in source) — this is a "
        "spelling fix, not a data change."
    ),
)
def silver_hic(context: dg.AssetExecutionContext):
    all_years = []

    for year, cfg in YEAR_CONFIG.items():
        path = raw_path_for_year(year)
        df = pd.read_excel(path, sheet_name=cfg["sheet"])

        for col in DROP_COLUMNS:
            if col in df.columns:
                df = df.drop(columns=[col])

        df = df.rename(columns=COLUMN_RENAME)

        for date_col in DATE_COLUMNS:
            if date_col in df.columns:
                df[date_col] = df[date_col].apply(
                    lambda v: v.isoformat() if isinstance(v, pd.Timestamp) else v
                )

        for geo_col in GEO_COLUMNS:
            if geo_col in df.columns:
                df[geo_col] = df[geo_col].astype(str)

        # Force all remaining object-dtype (text) columns to string too —
        # mixed-type inference on sparsely-populated text columns (e.g.
        # Organization Name) can otherwise trip up the parquet writer.
        # Replace actual NaN with None first so it doesn't become the
        # literal string "nan" after the cast.
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].where(df[col].notna(), None).astype(str)
            df.loc[df[col] == "None", col] = None

        df["year"] = year

        context.log.info(f"{year}: parsed {len(df)} rows from sheet '{cfg['sheet']}'")
        all_years.append(df)

    combined = pd.concat(all_years, ignore_index=True, sort=False)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(SILVER_PATH, index=False)

    context.log.info(f"Silver HIC file written to: {SILVER_PATH}")
    context.log.info(f"Total row count across all years: {len(combined)}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(combined)),
            "years_included": dg.MetadataValue.text(str(sorted(YEAR_CONFIG.keys()))),
            "output_path": dg.MetadataValue.path(str(SILVER_PATH)),
        }
    )