"""
ACS 5-Year Detailed Table Bronze Ingestion
--------------------------------------------
Pulls estimate (E) + margin of error (M) pairs for a configurable list of
Census ACS B-tables at tract level, checks for a collapsed C-table
counterpart, and lands raw long-format output to bronze.

No transformation, no MOE filtering, no derived rates here — bronze is a
faithful mirror of the API response. Reliability flagging (CV thresholds)
belongs in the silver layer.

Requires: CENSUS_API_KEY environment variable (free at
https://api.census.gov/data/key_signup.html)
"""

import os
import csv
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("acs_bronze_ingest")

API_BASE = "https://api.census.gov/data"
MAX_VARS_PER_CALL = 50  # Census API hard limit, including the 'for' geography variable
REQUEST_PAUSE_SECONDS = 0.3  # polite throttling; adjust for full historical backfills

STATE_FIPS = "06"     # California
COUNTY_FIPS = "037"   # Los Angeles County


def load_table_config(path: str) -> list[dict]:
    """Load the table_id/series config. One row per B-table to ingest."""
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def get_table_variables(table_id: str, year: int, api_key: str) -> list[str]:
    """
    Discover every E/M variable belonging to a table via the ACS variables
    endpoint, rather than hardcoding variable counts per table (which differ
    table to table and can change across vintages).
    """
    url = f"{API_BASE}/{year}/acs/acs5/variables.json"
    # The variables endpoint is large; in production cache this per year
    # rather than refetching per table. Left simple here for clarity.
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    all_vars = resp.json().get("variables", {})
    table_vars = [
        v for v in all_vars
        if v.startswith(table_id + "_") and (v.endswith("E") or v.endswith("M"))
    ]
    return sorted(table_vars)


def table_exists(table_id: str, year: int, api_key: str) -> bool:
    """Cheap existence check: does the table have at least one variable?"""
    try:
        return len(get_table_variables(table_id, year, api_key)) > 0
    except requests.HTTPError:
        return False


def chunk_variables(variables: list[str], max_size: int) -> list[list[str]]:
    """Census API caps variables per call; paginate wide tables."""
    # Reserve one slot for the 'NAME' variable we always request alongside.
    step = max_size - 1
    return [variables[i:i + step] for i in range(0, len(variables), step)]


def fetch_table_chunk(table_id: str, variables: list[str], year: int,
                       api_key: str) -> list[dict]:
    """Fetch one chunk of variables for a table at tract level for the configured county."""
    var_str = "NAME," + ",".join(variables)
    params = {
        "get": var_str,
        "for": "tract:*",
        "in": f"state:{STATE_FIPS} county:{COUNTY_FIPS}",
        "key": api_key,
    }
    url = f"{API_BASE}/{year}/acs/acs5"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    rows = resp.json()
    header, *data_rows = rows
    return [dict(zip(header, row)) for row in data_rows]


def melt_to_long(rows: list[dict], table_id: str, year: int) -> list[dict]:
    """
    Convert wide API response rows into long format:
    one row per (tract, variable) with estimate/moe split into columns.
    Long format keeps schema stable across all 86 tables despite each
    table having a different number/shape of columns.
    """
    long_rows = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    for row in rows:
        tract_fips = row.get("state", "") + row.get("county", "") + row.get("tract", "")
        tract_name = row.get("NAME", "")
        for key, value in row.items():
            if key.startswith(table_id + "_") and key.endswith("E"):
                var_root = key[:-1]  # strip trailing E
                moe_key = var_root + "M"
                long_rows.append({
                    "table_id": table_id,
                    "variable": var_root,
                    "tract_fips": tract_fips,
                    "tract_name": tract_name,
                    "year": year,
                    "estimate": value,
                    "moe": row.get(moe_key),
                    "ingested_at": ingested_at,
                })
    return long_rows


def write_bronze(rows: list[dict], table_id: str, year: int, out_dir: str):
    """Land raw long-format rows to bronze, partitioned by table and year."""
    if not rows:
        log.warning(f"No rows returned for {table_id} ({year}); skipping write.")
        return
    out_path = Path(out_dir) / f"table_id={table_id}" / f"year={year}"
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "data.csv"
    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Wrote {len(rows)} rows -> {out_file}")


def ingest_table(table_id: str, year: int, api_key: str, out_dir: str,
                  check_collapsed: bool = True):
    """Full ingestion for one B-table: fetch, melt, write. Also checks for a C-table."""
    log.info(f"Ingesting {table_id} ({year})")
    variables = get_table_variables(table_id, year, api_key)
    if not variables:
        log.warning(f"{table_id}: no variables found, skipping (check table ID/vintage).")
        return

    all_rows = []
    for chunk in chunk_variables(variables, MAX_VARS_PER_CALL):
        chunk_rows = fetch_table_chunk(table_id, chunk, year, api_key)
        all_rows.extend(chunk_rows)
        time.sleep(REQUEST_PAUSE_SECONDS)

    long_rows = melt_to_long(all_rows, table_id, year)
    write_bronze(long_rows, table_id, year, out_dir)

    if check_collapsed:
        c_table_id = "C" + table_id[1:]  # same number, C prefix
        if table_exists(c_table_id, year, api_key):
            log.info(f"{table_id}: collapsed version {c_table_id} found, ingesting too.")
            ingest_table(c_table_id, year, api_key, out_dir, check_collapsed=False)
        else:
            log.info(f"{table_id}: no collapsed version ({c_table_id} not found).")


def main():
    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        raise RuntimeError("Set the CENSUS_API_KEY environment variable before running.")

    year = int(os.environ.get("ACS_YEAR", "2023"))  # latest 5-year vintage at write time
    config_path = os.environ.get("TABLE_CONFIG_PATH", "table_config.csv")
    out_dir = os.environ.get("BRONZE_OUT_DIR", "./bronze/acs")

    tables = load_table_config(config_path)
    log.info(f"Loaded {len(tables)} tables from {config_path}")

    for row in tables:
        table_id = row["table_id"].strip()
        try:
            ingest_table(table_id, year, api_key, out_dir)
        except Exception as e:
            log.error(f"Failed to ingest {table_id}: {e}")
            continue


if __name__ == "__main__":
    main()
