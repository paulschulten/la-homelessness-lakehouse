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

# CDE renamed this district partway through the years covered here — same
# entity, not two different districts. Normalize to one canonical name so
# a district's multi-year trajectory doesn't fragment in two.
DISTRICT_RENAME = {
    "Whittier City Elementary": "Whittier City",
}

# Entities that appear at Aggregate Level = 'D' (district grain) but are NOT
# geographic school districts, and would skew any district-level ranking or
# comparison if mixed in unmarked:
#   - Los Angeles County Office of Education: a county administrative body,
#     not a district — its enrollment overlaps with real districts, causing
#     district-level sums to run higher than the true county total if not
#     excluded from analysis.
#   - 'SBE - ...' entries: individual charter schools directly authorized by
#     the State Board of Education, not geographic districts — tiny by
#     comparison to a real district, would distort a district comparison.
# Rows are NOT dropped (faithful ingestion) — marked via entity_type instead,
# so any analysis can filter as needed while the full data stays intact.
COUNTY_OFFICE_NAMES = {"Los Angeles County Office of Education"}


def _entity_type(row) -> str:
    if row["Aggregate Level"] != "D":
        return None
    name = row["District Name"]
    if name in COUNTY_OFFICE_NAMES:
        return "county_office"
    if isinstance(name, str) and name.startswith("SBE - "):
        return "sbe_charter_school"
    return "school_district"


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
        "entirely by the LA County filter, as expected). District Name "
        "normalized for a known CDE rename ('Whittier City Elementary' -> "
        "'Whittier City') so that district's trajectory doesn't fragment "
        "across years. New 'Entity Type' column (district-grain rows only) "
        "marks 'county_office' (LA County Office of Education — not a real "
        "district, its enrollment overlaps with real districts) and "
        "'sbe_charter_school' (individual State Board of Education charter "
        "schools that appear at district grain but aren't geographic "
        "districts) so a district-level ranking/comparison can filter these "
        "out without needing to know the source's specific entity names — "
        "no rows are dropped here, only marked."
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

        if "District Name" in df.columns:
            df["District Name"] = df["District Name"].replace(DISTRICT_RENAME)

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

        df["Entity Type"] = df.apply(_entity_type, axis=1)

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