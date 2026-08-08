# lh_orch/lh_assets/bronze_311.py

from pathlib import Path

import dagster as dg
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "02_data" / "01_raw" / "lacity" / "03_311_encampment"

# One Socrata dataset ID per year — LA's open data portal publishes MyLA311
# service requests as a separate dataset per year, not one continuous table.
# 2025 excluded: LA's own published dataset (h73f-gn57) stops around
# Jan 25, 2025 and hasn't been updated since — confirmed as a genuine
# staleness/publishing gap in the source (not an ingestion bug; Socrata's
# own count(*) matches the ~24k rows we pull, and the raw CSV export shows
# the same cutoff). Re-add once LA's portal catches the dataset back up.
DATASET_IDS = {
    2020: "rq3b-xjk8",
    2021: "97z7-y5bt",
    2022: "i5ke-k6by",
    2023: "4a4x-mna2",
    2024: "b7dx-7gc3",
    # 2025: "h73f-gn57",  # excluded — source dataset stale, see comment above
}

SOCRATA_BASE = "https://data.lacity.org/resource/{dataset_id}.json"

# NOTE: the exact column name Socrata uses for request type/category has not
# been confirmed against the live schema yet (it may be "requesttype",
# "RequestType", "requesttype_desc", or similar, and could differ by year's
# dataset). Before running this for real, fetch one page without a $where
# filter and inspect the field names:
#   requests.get(SOCRATA_BASE.format(dataset_id=DATASET_IDS[2024]),
#                params={"$limit": 5}).json()
# then set REQUEST_TYPE_FIELD and REQUEST_TYPE_VALUE below to match.
# Confirmed against the live 2024 dataset schema (both field name and label
# value verified via a $select/$group distinct-values query before this was
# locked in).
REQUEST_TYPE_FIELD = "requesttype"
REQUEST_TYPE_VALUE = "Homeless Encampment"

PAGE_SIZE = 50000  # Socrata's practical max per page


def raw_path_for_year(year: int) -> Path:
    return BRONZE_DIR / f"encampment_311_{year}.json"


def _fetch_year(year: int, dataset_id: str, context: dg.AssetExecutionContext) -> list[dict]:
    url = SOCRATA_BASE.format(dataset_id=dataset_id)
    where_clause = f"{REQUEST_TYPE_FIELD} = '{REQUEST_TYPE_VALUE}'"

    # Get the true total up front. Socrata's SODA API can silently return a
    # partial page for an expensive $where-filtered query without erroring —
    # a page shorter than $limit does NOT reliably mean "no more data," so we
    # can't trust that as the sole stop condition (this caused 2025 to
    # silently truncate to ~24k rows out of a much larger true total on an
    # earlier run). Comparing the final row count against this expected
    # total catches that failure mode instead of trusting page length alone.
    count_resp = requests.get(
        url, params={"$where": where_clause, "$select": "count(*)"}, timeout=120
    )
    count_resp.raise_for_status()
    expected_total = int(count_resp.json()[0]["count"])
    context.log.info(f"{year}: expected total rows = {expected_total}")

    all_rows = []
    offset = 0

    while True:
        params = {
            "$where": where_clause,
            "$order": "srnumber",  # stable sort required for correct offset pagination
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break

        all_rows.extend(page)
        context.log.info(f"{year}: fetched {len(page)} rows (offset {offset})")
        offset += PAGE_SIZE

        if len(all_rows) >= expected_total:
            break

    if len(all_rows) != expected_total:
        context.log.warning(
            f"{year}: fetched {len(all_rows)} rows but expected {expected_total} — "
            "possible incomplete pull, investigate before trusting this year's data."
        )

    return all_rows


@dg.asset(
    group_name="encampment_311",
    description=(
        "Raw LA City MyLA311 homeless encampment service requests, per year, "
        "downloaded via the Socrata API and saved as-is. Citizen-reported "
        "requests (phone/app/web) flagging encampments — includes lat/long, "
        "street address, zip, council district, neighborhood council, and "
        "police precinct. Filtered from the full 311 feed on "
        "requesttype = 'Homeless Encampment' (confirmed against the live "
        "schema before use)."
    ),
)
def bronze_311_encampment(context: dg.AssetExecutionContext):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    year_counts = {}

    for year, dataset_id in DATASET_IDS.items():
        out_path = raw_path_for_year(year)
        if out_path.exists():
            context.log.info(f"{year}: already present at {out_path}, skipping download")
            continue

        context.log.info(f"{year}: fetching from dataset {dataset_id}")
        rows = _fetch_year(year, dataset_id, context)

        import json
        out_path.write_text(json.dumps(rows))
        year_counts[year] = len(rows)
        context.log.info(f"{year}: saved {len(rows)} rows to {out_path}")

    return dg.MaterializeResult(
        metadata={
            "year_counts": dg.MetadataValue.text(str(year_counts)),
            "bronze_dir": dg.MetadataValue.path(str(BRONZE_DIR)),
        }
    )