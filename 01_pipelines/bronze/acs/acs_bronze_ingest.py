"""
ACS 5-Year Detailed Table Bronze Ingestion
--------------------------------------------
Pulls estimate (E) + margin of error (M) pairs for a configurable list of
Census ACS B-tables at tract level, checks for a collapsed C-table
counterpart, captures human-readable variable labels, and lands raw
long-format output to an Iceberg bronze table.

No MOE filtering, no derived rates here — bronze is a faithful mirror of
the API response (plus labels, captured here since we're already hitting
the variables endpoint). Reliability flagging (CV thresholds) belongs in
the silver layer.

Requires: CENSUS_API_KEY environment variable (free at
https://api.census.gov/data/key_signup.html)
"""

import os
import csv
import time
import logging
import requests
import pyarrow as pa
from datetime import datetime, timezone

from iceberg_catalog import get_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("acs_bronze_ingest")

API_BASE = "https://api.census.gov/data"
MAX_VARS_PER_CALL = 50       # Census API hard limit, including the 'for' geography variable
REQUEST_PAUSE_SECONDS = 0.3  # polite throttling; adjust for full historical backfills
MAX_RETRIES = 3
RETRY_BASE_DELAY_SECONDS = 2  # doubles each retry: 2s, 4s, 8s

STATE_FIPS = "06"     # California
COUNTY_FIPS = "037"   # Los Angeles County

ICEBERG_NAMESPACE = "bronze"
ICEBERG_TABLE_NAME = "acs_estimates"

BRONZE_SCHEMA = pa.schema([
    ("table_id", pa.string()),
    ("variable", pa.string()),
    ("variable_label", pa.string()),
    ("tract_fips", pa.string()),
    ("tract_name", pa.string()),
    ("year", pa.int32()),
    ("estimate", pa.string()),
    ("moe", pa.string()),
    ("ingested_at", pa.string()),
])


def load_table_config(path: str) -> list[dict]:
    """Load the table_id/series config. One row per B-table to ingest.

    The config file is tab-delimited (despite the .csv extension), so we
    tell csv.DictReader to split on tabs rather than assume commas.
    """
    with open(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def request_with_retry(url: str, params: dict | None = None) -> requests.Response:
    """
    GET with exponential backoff. A single transient failure (timeout, 5xx,
    rate limit) shouldn't kill an 85-table run — retry a few times before
    giving up and letting the caller log/skip this table.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"Retryable status {resp.status_code}")
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            # Non-retryable client error (bad variable name, malformed
            # request, etc.) - retrying won't change the outcome, fail fast.
            if e.response is not None and e.response.status_code not in (429,) \
                    and e.response.status_code < 500:
                raise
            last_exc = e
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                log.warning(f"Request failed (attempt {attempt}/{MAX_RETRIES}): {e}. "
                            f"Retrying in {delay}s...")
                time.sleep(delay)
        except requests.RequestException as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                log.warning(f"Request failed (attempt {attempt}/{MAX_RETRIES}): {e}. "
                            f"Retrying in {delay}s...")
                time.sleep(delay)
    raise last_exc


def get_table_variables_with_labels(table_id: str, year: int) -> dict[str, str]:
    """
    Discover every E/M variable belonging to a table via the ACS variables
    endpoint, along with its human-readable label. Returns {variable_code: label}
    for the *estimate* variables only (E-suffixed); the matching M variable
    is assumed to share the same root.
    """
    url = f"{API_BASE}/{year}/acs/acs5/variables.json"
    resp = request_with_retry(url)
    all_vars = resp.json().get("variables", {})
    table_vars = {}
    for code, meta in all_vars.items():
        if code.startswith(table_id + "_") and code.endswith("E"):
            table_vars[code[:-1]] = meta.get("label", "")  # store root, strip E
    return table_vars


def table_exists(table_id: str, year: int) -> bool:
    """Cheap existence check: does the table have at least one variable?"""
    try:
        return len(get_table_variables_with_labels(table_id, year)) > 0
    except requests.HTTPError:
        return False


def chunk_variables(roots: list[str], max_size: int) -> list[list[str]]:
    """
    Census API caps variables per call, including NAME. Each root expands to
    two variables on the wire (its E and M codes), so a chunk of N roots
    costs 2N + 1 slots. Reserve one for NAME and fit floor((max_size-1)/2)
    roots per chunk.
    """
    step = (max_size - 1) // 2
    return [roots[i:i + step] for i in range(0, len(roots), step)]


def fetch_table_chunk(roots: list[str], year: int, api_key: str) -> list[dict]:
    """Fetch one chunk of variables (E and M) for a table at tract level."""
    codes = []
    for root in roots:
        codes.append(root + "E")
        codes.append(root + "M")
    var_str = "NAME," + ",".join(codes)
    params = {
        "get": var_str,
        "for": "tract:*",
        "in": f"state:{STATE_FIPS} county:{COUNTY_FIPS}",
        "key": api_key,
    }
    url = f"{API_BASE}/{year}/acs/acs5"
    resp = request_with_retry(url, params=params)
    rows = resp.json()
    header, *data_rows = rows
    return [dict(zip(header, row)) for row in data_rows]


def melt_to_long(rows: list[dict], table_id: str, year: int,
                  var_labels: dict[str, str]) -> list[dict]:
    """
    Convert wide API response rows into long format: one row per
    (tract, variable) with estimate/moe split into columns and the
    variable's human-readable label attached.
    """
    long_rows = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    for row in rows:
        tract_fips = row.get("state", "") + row.get("county", "") + row.get("tract", "")
        tract_name = row.get("NAME", "")
        for key, value in row.items():
            if key.startswith(table_id + "_") and key.endswith("E"):
                var_root = key[:-1]
                moe_key = var_root + "M"
                long_rows.append({
                    "table_id": table_id,
                    "variable": var_root,
                    "variable_label": var_labels.get(var_root, ""),
                    "tract_fips": tract_fips,
                    "tract_name": tract_name,
                    "year": year,
                    "estimate": value,
                    "moe": row.get(moe_key),
                    "ingested_at": ingested_at,
                })
    return long_rows


def get_or_create_bronze_table(catalog):
    """Get the Iceberg bronze table, creating it (and its namespace) on first run."""
    if (ICEBERG_NAMESPACE,) not in [ns for ns in catalog.list_namespaces()]:
        catalog.create_namespace(ICEBERG_NAMESPACE)
    identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
    if not catalog.table_exists(identifier):
        return catalog.create_table(identifier, schema=BRONZE_SCHEMA)
    return catalog.load_table(identifier)


def write_bronze(rows: list[dict], iceberg_table):
    """Append rows to the Iceberg bronze table."""
    if not rows:
        log.warning("No rows to write; skipping.")
        return
    arrow_table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
    iceberg_table.append(arrow_table)
    log.info(f"Appended {len(rows)} rows to Iceberg bronze table.")


def ingest_table(table_id: str, year: int, api_key: str, iceberg_table,
                  check_collapsed: bool = True):
    """Full ingestion for one B-table: fetch, melt, write. Also checks for a C-table."""
    log.info(f"Ingesting {table_id} ({year})")
    var_labels = get_table_variables_with_labels(table_id, year)
    if not var_labels:
        log.warning(f"{table_id}: no variables found, skipping (check table ID/vintage).")
        return

    all_rows = []
    for chunk in chunk_variables(list(var_labels.keys()), MAX_VARS_PER_CALL):
        chunk_rows = fetch_table_chunk(chunk, year, api_key)
        all_rows.extend(chunk_rows)
        time.sleep(REQUEST_PAUSE_SECONDS)

    long_rows = melt_to_long(all_rows, table_id, year, var_labels)
    write_bronze(long_rows, iceberg_table)

    if check_collapsed:
        c_table_id = "C" + table_id[1:]  # same number, C prefix
        if table_exists(c_table_id, year):
            log.info(f"{table_id}: collapsed version {c_table_id} found, ingesting too.")
            ingest_table(c_table_id, year, api_key, iceberg_table, check_collapsed=False)
        else:
            log.info(f"{table_id}: no collapsed version ({c_table_id} not found).")

def run_acs_bronze_ingestion(api_key: str, year: int, config_path: str) -> dict:
    """Run bronze ingestion for every table in config_path. Returns a summary dict."""
    tables = load_table_config(config_path)
    log.info(f"Loaded {len(tables)} tables from {config_path}")

    catalog = get_catalog()
    iceberg_table = get_or_create_bronze_table(catalog)

    tables_processed = 0
    failed_table_ids = []

    for row in tables:
        table_id = row["table_id"].strip()
        try:
            ingest_table(table_id, year, api_key, iceberg_table)
            tables_processed += 1
        except Exception as e:
            log.error(f"Failed to ingest {table_id}: {e}")
            failed_table_ids.append(table_id)
            continue

    return {
        "tables_configured": len(tables),
        "tables_processed": tables_processed,
        "tables_failed": len(failed_table_ids),
        "failed_table_ids": failed_table_ids,
    }


def main():
    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        raise RuntimeError("Set the CENSUS_API_KEY environment variable before running.")

    year = int(os.environ.get("ACS_YEAR", "2023"))  # latest 5-year vintage at write time
    config_path = os.environ.get("TABLE_CONFIG_PATH", "table_config.csv")

    summary = run_acs_bronze_ingestion(api_key=api_key, year=year, config_path=config_path)
    log.info(
        f"Done. Processed {summary['tables_processed']}/{summary['tables_configured']} "
        f"tables ({summary['tables_failed']} failed)."
    )

if __name__ == "__main__":
    main()

