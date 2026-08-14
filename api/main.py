# api/main.py

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from pathlib import Path

import duckdb
import numpy as np
import sqlglot
from sqlglot import exp
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

app = FastAPI(title="LA Homelessness Lakehouse API")

ICEBERG_NAMESPACE = "gold"

# --- Audit logging --------------------------------------------------------
# Every /query request is logged: timestamp, the cleaned SQL, row count
# returned, execution time, and success/failure. Writes to a local file
# (rotates would be a future improvement) so there's a record of who ran
# what, when — useful for debugging, understanding real usage patterns,
# and eventually billing/tiering once auth exists.
_audit_logger = logging.getLogger("query_audit")
_audit_logger.setLevel(logging.INFO)
_audit_handler = logging.FileHandler(PROJECT_ROOT / "api" / "query_audit.log")
_audit_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
_audit_logger.addHandler(_audit_handler)

# Small allowlist of queryable tables, rather than accepting any string —
# a first, minimal safety boundary. Expand as more gold tables are ready
# to expose.
ALLOWED_TABLES = {
    "fact_pit_count",
    "fact_hic",
    "dim_hic_project",
    "fact_311_encampment",
    "fact_homeless_students",
    "fact_acs_estimates",
    "dim_tract",
}


@app.get("/")
def root():
    return {"status": "ok", "service": "LA Homelessness Lakehouse API"}


@app.get("/tables/{table_name}")
def get_table(table_name: str, limit: int = 5):
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' is not available. Allowed: {sorted(ALLOWED_TABLES)}",
        )

    if limit > 1000:
        limit = 1000  # basic guardrail — real pagination comes later

    catalog = get_catalog()
    identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
    table = catalog.load_table(identifier)

    df = table.scan(limit=limit).to_pandas()

    # Standard JSON doesn't allow NaN — real nulls in the source data
    # (e.g. fact_pit_count.totpeople for 2023+) come through pandas as NaN
    # and need to become proper JSON null (Python None) before serializing.
    df = df.replace({np.nan: None})

    return {
        "table": table_name,
        "row_count": len(df),
        "rows": df.to_dict(orient="records"),
    }


# --- Raw SQL query endpoint ---------------------------------------------
#
# Safety restrictions, tuned to be invisible to a legitimate query:
#   - SELECT-only: the query is parsed into a real SQL syntax tree (via
#     sqlglot) and checked structurally — is this actually a SELECT
#     statement — rather than scanning the raw text for keywords. This
#     avoids false positives like WHERE status = 'SET' being rejected
#     just because the word SET appears inside a string value.
#   - Row cap: 100 rows — plenty for a person reviewing results by hand
#     during testing; no meaningful AWS cost difference vs. a higher cap,
#     since cost is driven by data scanned to answer the query, not rows
#     returned.
#   - Timeout: generous (30s) — protects against a truly runaway query
#     without cutting off a legitimate slower aggregation.
# NOT yet in place: authentication / per-user rate limiting. Fine for a
# small trusted beta group; required before any public rollout.

MAX_ROWS = 100
QUERY_TIMEOUT_SECONDS = 30

_executor = ThreadPoolExecutor(max_workers=4)


def _validate_select_only(sql: str) -> str:
    """Returns the cleaned query if it's structurally a single SELECT
    statement, else raises. Uses a real SQL parser rather than text
    matching, so it never false-positives on a keyword that happens to
    appear inside a string literal or column value."""
    cleaned = sql.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()

    try:
        statements = sqlglot.parse(cleaned, read="duckdb")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse query: {e}")

    if len(statements) != 1:
        raise HTTPException(
            status_code=400,
            detail="Submit exactly one query at a time.",
        )

    statement = statements[0]

    # A CTE (WITH ...) wraps a SELECT — unwrap to check the real statement
    # type. Anything else (Drop, Delete, Insert, Update, Create, Pragma,
    # Command, etc.) is rejected by simply not matching Select/With here.
    is_select = isinstance(statement, exp.Select)
    is_select_cte = isinstance(statement, exp.With) and isinstance(
        statement.this, exp.Select
    )

    if not (is_select or is_select_cte):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries are allowed (queries may start with WITH for a CTE).",
        )

    # Gold-only restriction: walk every table reference in the parsed
    # query. Reject anything qualified with a catalog/db other than the
    # plain, unqualified table name (i.e. reject glue_catalog.gold.x,
    # glue_catalog.silver.x, information_schema.x, etc. entirely) and
    # reject any unqualified name that isn't in ALLOWED_TABLES. This
    # forces queries to use the same plain table names set up as views
    # below — SELECT * FROM fact_pit_count, same as client.duckdb —
    # while making it impossible to reach silver/bronze or any other
    # schema by writing it out explicitly.
    for table in statement.find_all(exp.Table):
        if table.catalog or table.db:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Table references must use plain names (e.g. '{table.name}'), "
                    "not schema-qualified paths. Only gold-layer tables are queryable."
                ),
            )
        if table.name not in ALLOWED_TABLES:
            raise HTTPException(
                status_code=400,
                detail=f"Table '{table.name}' is not available. Allowed: {sorted(ALLOWED_TABLES)}",
            )

    return cleaned


class QueryRequest(BaseModel):
    sql: str


# --- Shared DuckDB connection ---------------------------------------------
# The INSTALL/LOAD/ATTACH/CREATE VIEW setup was previously repeated on
# every single request — each one re-attaching the Glue catalog and
# re-creating 7 views, each requiring a fresh metadata fetch from S3.
# That was the actual cause of ~10s response times. Doing this setup once,
# at server startup, and reusing one shared connection across all requests
# removes that repeated cost entirely; requests should now only pay for
# the cost of their own query, not the setup.
#
# Concurrency: each request gets its own cursor via con.cursor() rather
# than sharing the base connection object directly — this gives each
# request an independent client to the same in-memory database, so
# concurrent requests don't block or interfere with each other, and a
# per-request timeout/interrupt only affects that request's own cursor.
_shared_con = None


def _init_shared_connection():
    global _shared_con
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")
    con.sql("CREATE SECRET IF NOT EXISTS (TYPE s3, PROVIDER credential_chain, CHAIN 'config');")
    con.sql(
        """
        ATTACH IF NOT EXISTS '277607772876' AS glue_catalog (
            TYPE iceberg,
            ENDPOINT 'glue.us-east-2.amazonaws.com/iceberg',
            AUTHORIZATION_TYPE 'sigv4'
        );
        """
    )
    for table_name in ALLOWED_TABLES:
        con.sql(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM glue_catalog.gold.{table_name}"
        )
    _shared_con = con


@app.on_event("startup")
def _on_startup():
    _init_shared_connection()


def _execute_capped(cur, cleaned_sql: str, max_rows: int):
    wrapped = f"SELECT * FROM ({cleaned_sql}) AS user_query LIMIT {max_rows}"
    result = cur.sql(wrapped)
    return result.to_df() if result is not None else None


@app.post("/query")
def run_query(request: QueryRequest):
    start = time.monotonic()
    status = "error"
    row_count = 0

    try:
        cleaned_sql = _validate_select_only(request.sql)

        cur = _shared_con.cursor()
        future = _executor.submit(_execute_capped, cur, cleaned_sql, MAX_ROWS)

        try:
            df = future.result(timeout=QUERY_TIMEOUT_SECONDS)
        except FutureTimeoutError:
            cur.interrupt()
            raise HTTPException(
                status_code=408,
                detail=f"Query exceeded the {QUERY_TIMEOUT_SECONDS}s time limit and was cancelled.",
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Query failed: {e}")
        finally:
            cur.close()

        if df is None:
            row_count = 0
            status = "success"
            return {"row_count": 0, "rows": []}

        df = df.replace({np.nan: None})
        row_count = len(df)
        status = "success"

        return {
            "row_count": row_count,
            "truncated": row_count == MAX_ROWS,
            "rows": df.to_dict(orient="records"),
        }

    except HTTPException as e:
        status = f"rejected ({e.status_code})"
        raise
    finally:
        duration_ms = round((time.monotonic() - start) * 1000, 1)
        _audit_logger.info(
            f"status={status} duration_ms={duration_ms} row_count={row_count} sql={request.sql!r}"
        )