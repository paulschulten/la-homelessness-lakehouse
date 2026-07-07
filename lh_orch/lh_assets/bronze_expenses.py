import os
import json
import requests
from datetime import datetime
from pathlib import Path
from dagster import asset

DATA_URL = "https://controllerdata.lacity.org/resource/98ve-cuf5.json"
PAGE_SIZE = 50000

project_root = Path(__file__).resolve().parents[2]

DATA_DIR = project_root / "02_data"
BRONZE_DIR = DATA_DIR / "01_raw" / "lacity" / "01_homelessness_expenses"

def fetch_all_pages():
    all_rows = []
    offset = 0

    while True:
        response = requests.get(
            DATA_URL,
            params={
                "$limit": PAGE_SIZE,
                "$offset": offset
            }
        )
        response.raise_for_status()

        page = response.json()

        if not page:
            break

        all_rows.extend(page)
        offset += PAGE_SIZE

    return all_rows

@asset
def bronze_expenses():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    data = fetch_all_pages()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"homelessness_expenses_full_{timestamp}.json"
    output_path = BRONZE_DIR / filename

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    return str(output_path)
