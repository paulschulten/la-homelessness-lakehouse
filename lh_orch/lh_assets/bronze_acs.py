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

from acs_bronze_ingest import run_acs_bronze_ingestion  # noqa: E402

TABLE_CONFIG_PATH = str(ACS_DIR / "table_config.csv")

@dg.asset(
    name="bronze_acs_estimates",
    description=(
        "ACS 5-year tables with margins of error."
    ),
)
def bronze_acs_estimates(context: dg.AssetExecutionContext):
    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        raise RuntimeError("CENSUS_API_KEY is not set in the Dagster environment.")

    year = int(os.environ.get("ACS_YEAR", "2023"))

    summary = run_acs_bronze_ingestion(
        api_key=api_key,
        year=year,
        config_path=TABLE_CONFIG_PATH,
    )

    context.log.info(
        f"ACS bronze ingestion complete: "
        f"{summary['tables_processed']}/{summary['tables_configured']} tables processed, "
        f"{summary['tables_failed']} failed."
    )

    return dg.MaterializeResult(
        metadata={
            "tables_configured": dg.MetadataValue.int(summary["tables_configured"]),
            "tables_processed": dg.MetadataValue.int(summary["tables_processed"]),
            "tables_failed": dg.MetadataValue.int(summary["tables_failed"]),
            "failed_table_ids": dg.MetadataValue.text(", ".join(summary["failed_table_ids"]) or "none"),
        }
    )