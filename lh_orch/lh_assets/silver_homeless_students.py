# lh_orch/lh_assets/silver_homeless_students.py

from pathlib import Path

import dagster as dg
import pandas as pd

from lh_orch.lh_assets.bronze_homeless_students import (
    FILE_URLS,
    bronze_homeless_students,
    raw_path_for_year,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "02_data" / "02_silver" / "cde" / "01_homeless_student_enrollment"
SILVER_PATH = SILVER_DIR / "homeless_students_silver.parquet"

COUNT_COLUMNS = [
    "Cumulative Enrollment",
    "Homeless Student Enrollment",
    "Temporarily Doubled Up",
    "Temporary Shelters",
    "Hotels/Motels",
    "Temporarily Unsheltered",
    "Missing/Unknown",
]

PERCENT_COLUMNS = [
    "Temporarily Doubled Up (percent)",
    "Temporary Shelters (percent)",
    "Hotels/Motels (percent)",
    "Temporarily Unsheltered (percent)",
    "Missing/Unknown (percent)",
]

TEXT_COLUMNS = [
    "Academic Year", "Aggregate Level", "County Code", "District Code",
    "School Code", "County Name", "District Name", "School Name",
    "Charter School", "DASS", "Reporting Category",
]


@dg.asset(
    deps=[bronze_homeless_students],
    group_name="homeless_students",
    description=(
        "Cleaned CDE Homeless Student Enrollment by Dwelling Type data, "
        "filtered to County Name = 'Los Angeles' (every LA County district "
        "and school, plus the county-level total row), 2019-20 through "
        "2024-25. Suppressed cells (source used a literal '*' for student-"
        "privacy suppression, cell size <= 10) converted to real null, not "
        "left as text — count and percent columns are numeric. Aggregate "
        "Level column distinguishes county- vs. district- vs. school-level "
        "rows within this LA-filtered set (state-level 'T' rows are dropped "
        "entirely by the LA County filter, as expected)."
    ),
)
def silver_homeless_students(context: dg.AssetExecutionContext):
    all_years = []

    for academic_year in FILE_URLS.keys():
        path = raw_path_for_year(academic_year)
        df = pd.read_csv(path, sep="\t", dtype=str, encoding="latin-1")

        # Some years' files (e.g. 2023-24) carry a UTF-8 byte-order-mark
        # that, under latin-1 decoding, becomes a literal "ï»¿" glued onto
        # the first column's name (e.g. "ï»¿Academic Year" instead of
        # "Academic Year"). Strip it so every year's columns line up
        # consistently after concat, regardless of which years have it.
        df.columns = df.columns.str.replace("ï»¿", "", regex=False)

        before = len(df)
        df = df[df["County Name"].str.strip() == "Los Angeles"].copy()
        context.log.info(
            f"{academic_year}: {before} rows loaded, {len(df)} rows after LA County filter"
        )

        # Suppressed cells are a literal '*' in the source, not blank — treat
        # as missing (real null), not as the string '*'.
        for col in COUNT_COLUMNS + PERCENT_COLUMNS:
            if col in df.columns:
                df[col] = df[col].replace("*", pd.NA)
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in TEXT_COLUMNS:
            if col in df.columns:
                df[col] = df[col].where(df[col].notna(), None).astype(str)
                df.loc[df[col] == "None", col] = None

        all_years.append(df)

    combined = pd.concat(all_years, ignore_index=True, sort=False)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(SILVER_PATH, index=False)

    context.log.info(f"Silver homeless students file written to: {SILVER_PATH}")
    context.log.info(f"Total row count across all years (LA County only): {len(combined)}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(combined)),
            "years_included": dg.MetadataValue.text(str(list(FILE_URLS.keys()))),
            "output_path": dg.MetadataValue.path(str(SILVER_PATH)),
        }
    )