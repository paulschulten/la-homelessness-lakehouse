# lh_orch/lh_assets/bronze_expenses.py

import dagster as dg
import requests
import json
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).resolve().parents[2]

RAW_DIR = (
    project_root
    / "02_data"
    / "01_raw"
    / "lacity"
    / "01_homelessness_expenses"
)

DATA_URL = "https://controllerdata.lacity.org/api/v3/views/98ve-cuf5/query.json"


@dg.asset(
    name="bronze_expenses",
    description="Raw homelessness expenses data pulled from LA Controller API view.",
)
def bronze_expenses(context: dg.AssetExecutionContext):

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    context.log.info(f"Downloading dataset from: {DATA_URL}")

    response = requests.get(DATA_URL, timeout=30)
    response.raise_for_status()

    # The payload is a LIST, not a dict
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Expected list-based JSON payload but received something else.")

    record_count = len(data)
    context.log.info(f"Fetched {record_count} records from LA Controller API view")

    # Write timestamped raw JSON file
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = RAW_DIR / f"homelessness_expenses_raw_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    context.log.info(f"Wrote Bronze file: {output_path}")

    return dg.MaterializeResult(
        metadata={
            "records": dg.MetadataValue.int(record_count),
            "output_path": dg.MetadataValue.path(str(output_path)),
        }
    )
