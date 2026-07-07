import os
import json
import requests
from datetime import datetime

# LA City Homelessness Expense Tracker endpoint
DATA_URL = "https://controllerdata.lacity.org/resource/98ve-cuf5.json"

RAW_DIR = "data/raw/homelessness_expense_lacity"
PAGE_SIZE = 50000  # Socrata max is usually 50k per page

def ensure_directories():
    os.makedirs(RAW_DIR, exist_ok=True)

def fetch_all_pages():
    all_rows = []
    offset = 0

    while True:
        print(f"Fetching rows {offset} to {offset + PAGE_SIZE}...")

        response = requests.get(
            DATA_URL,
            params={
                "$limit": PAGE_SIZE,
                "$offset": offset
            }
        )

        if response.status_code != 200:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

        page = response.json()

        if not page:
            print("No more rows returned. Pagination complete.")
            break

        all_rows.extend(page)
        offset += PAGE_SIZE

    print(f"Total rows fetched: {len(all_rows)}")
    return all_rows

def write_raw_file(data):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"homelessness_expense_lacity_full_{timestamp}.json"
    output_path = os.path.join(RAW_DIR, filename)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Raw file written: {output_path}")

def main():
    ensure_directories()
    data = fetch_all_pages()

    if not data:
        print("Warning: API returned zero rows. No file written.")
        return

    write_raw_file(data)
    print("Full historical Bronze ingestion complete.")

if __name__ == "__main__":
    main()
