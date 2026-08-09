# lh_orch/lh_assets/bronze_homeless_students.py

from pathlib import Path

import dagster as dg
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "02_data" / "01_raw" / "cde" / "01_homeless_student_enrollment"

# CDE publishes one flat, tab-delimited file per academic year — statewide,
# with state/county/district/school level rows all in one file (filter to
# County Name = "Los Angeles" downstream to get every LA County district and
# school in one pass, per confirmed file structure).
FILE_URLS = {
    "2019-20": "https://www3.cde.ca.gov/demo-downloads/homeless/hse1920.txt",
    "2020-21": "https://www3.cde.ca.gov/demo-downloads/homeless/hse2021.txt",
    "2021-22": "https://www3.cde.ca.gov/demo-downloads/homeless/hse2122.txt",
    "2022-23": "https://www3.cde.ca.gov/demo-downloads/homeless/hse2223.txt",
    "2023-24": "https://www3.cde.ca.gov/demo-downloads/homeless/hse2324.txt",
    "2024-25": "https://www3.cde.ca.gov/demo-downloads/homeless/hse2425.txt",
}


def raw_path_for_year(academic_year: str) -> Path:
    # e.g. "2024-25" -> hse_2024-25.txt
    return BRONZE_DIR / f"hse_{academic_year}.txt"


@dg.asset(
    group_name="homeless_students",
    description=(
        "Raw CDE Homeless Student Enrollment by Dwelling Type files, one per "
        "academic year (2019-20 through 2024-25), downloaded as-is. Statewide, "
        "tab-delimited, state/county/district/school level rows in one file "
        "per year (Aggregate Level column: T/C/D/S). Suppressed cells (student "
        "privacy, cell size <= 10) appear as a literal '*' in the source, not "
        "blank — handled downstream in silver, not here."
    ),
)
def bronze_homeless_students(context: dg.AssetExecutionContext):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = []

    for academic_year, url in FILE_URLS.items():
        out_path = raw_path_for_year(academic_year)
        if out_path.exists():
            context.log.info(f"{academic_year}: already present at {out_path}, skipping download")
            downloaded.append(academic_year)
            continue

        context.log.info(f"{academic_year}: downloading from {url}")
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        out_path.write_bytes(resp.content)
        context.log.info(f"{academic_year}: saved to {out_path} ({len(resp.content)} bytes)")
        downloaded.append(academic_year)

    return dg.MaterializeResult(
        metadata={
            "years_downloaded": dg.MetadataValue.text(str(downloaded)),
            "bronze_dir": dg.MetadataValue.path(str(BRONZE_DIR)),
        }
    )