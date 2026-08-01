# lh_orch/lh_assets/bronze_acs.py

import sys
import os
from pathlib import Path

import dagster as dg

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"
ACS_DIR = PIPELINES_DIR / "bronze" / "acs"

for path in (str(PIPELINES_DIR), str(ACS_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

from acs_bronze_ingest import run_acs_bronze_ingestion, clear_bronze_table  # noqa: E402

TABLE_CONFIG_PATH = str(ACS_DIR / "table_config.csv")

@dg.asset(
    name="bronze_acs_estimates",
    group_name="acs",
    description=(
        "ACS 5-year tables with margins of error."
    ),
)
def bronze_acs_estimates(context: dg.AssetExecutionContext):
    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        raise RuntimeError("CENSUS_API_KEY is not set in the Dagster environment.")

    years = [2020, 2021, 2022, 2023, 2024]

    clear_bronze_table()

    all_summaries = {}
    for year in years:
        context.log.info(f"Starting ACS bronze ingestion for year {year}")
        summary = run_acs_bronze_ingestion(
            api_key=api_key,
            year=year,
            config_path=TABLE_CONFIG_PATH,
        )
        all_summaries[year] = summary
        context.log.info(
            f"Year {year}: {summary['tables_processed']}/{summary['tables_configured']} tables processed, "
            f"{summary['tables_failed']} failed."
        )

    total_processed = sum(s["tables_processed"] for s in all_summaries.values())
    total_configured = sum(s["tables_configured"] for s in all_summaries.values())
    total_failed = sum(s["tables_failed"] for s in all_summaries.values())

    return dg.MaterializeResult(
        metadata={
            "years_ingested": dg.MetadataValue.text(str(years)),
            "tables_configured": dg.MetadataValue.int(total_configured),
            "tables_processed": dg.MetadataValue.int(total_processed),
            "tables_failed": dg.MetadataValue.int(total_failed),
        }
    )