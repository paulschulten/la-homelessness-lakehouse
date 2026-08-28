# api/main.py

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from pathlib import Path

import anthropic
import duckdb
import numpy as np
import sqlglot
from sqlglot import exp

import io

from mangum import Mangum

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINES_DIR = PROJECT_ROOT / "01_pipelines"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))

from iceberg_catalog import get_catalog  # noqa: E402

app = FastAPI(title="LA Homelessness Lakehouse API")
handler = Mangum(app, api_gateway_base_path="/prod")


def _strip_markdown_fence(text: str) -> str:
    """LLMs sometimes wrap SQL in a markdown code fence (```sql ... ```)
    even when explicitly told not to — a well-known, common failure mode
    that's more reliable to fix in code than to rely on prompt compliance
    alone. Strips a leading ```sql or ``` and a trailing ``` if present."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
        if cleaned.lower().startswith("sql"):
            cleaned = cleaned[3:]
        cleaned = cleaned.lstrip("\n")
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip()

# --- CORS ------------------------------------------------------------------
# Browsers block a page on one origin (http://localhost:3000, the Next.js
# dev server) from reading responses from a different origin
# (http://localhost:8000, this API) unless the server explicitly allows it.
# Without this, requests still reach FastAPI and execute (visible as 200 in
# the server log) but the browser discards the response before your page's
# JavaScript ever sees it — showing as "Failed to fetch" client-side. This
# allowlist is deliberately narrow (only the known local dev origin); widen
# it to the real deployed frontend URL once this API is actually deployed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://main.d2526iaxcengli.amplifyapp.com"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ICEBERG_NAMESPACE = "gold"

# --- Audit logging --------------------------------------------------------
# Every /query request is logged: timestamp, the cleaned SQL, row count
# returned, execution time, and success/failure. Writes to a local file
# (rotates would be a future improvement) so there's a record of who ran
# what, when — useful for debugging, understanding real usage patterns,
# and eventually billing/tiering once auth exists.
_audit_logger = logging.getLogger("query_audit")
_audit_logger.setLevel(logging.INFO)
_audit_handler = logging.FileHandler("/tmp/query_audit.log")
_audit_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
_audit_logger.addHandler(_audit_handler)

# Every queryable gold table, expanded to include the dim tables and the
# expenses data source that were missing before — plus which data source
# each one belongs to and a human-written description, since DuckDB's
# DESCRIBE only knows types, not meaning. ACS is the one exception: it has
# 1,924 variables, far too many to hand-describe here — dim_variable IS its
# data dictionary (variable code -> label), queryable directly instead.
TABLE_METADATA = {
    "ACS (Census demographics)": {
        "description": "Census Bureau 5-year American Community Survey estimates at the tract level, 2020-2024. 1,924 variables — too many to describe individually here; query dim_variable for the code-to-label lookup covering every variable.",
        "tables": {
            "fact_acs_estimates": {
                "description": "One row per tract, year, and variable code, with the estimate value.",
                "columns": {},
            },
            "dim_variable": {
                "description": "Lookup table: ACS variable code -> plain-language label. Use this to find which variable code you need before querying fact_acs_estimates.",
                "columns": {},
            },
            "dim_tract": {
                "description": "Census tract dimension — tract_fips and related geographic identifiers.",
                "columns": {},
            },
        },
    },
    "PIT Count (LAHSA)": {
        "description": "LAHSA's annual Point-in-Time count of people experiencing homelessness, by tract, 2020-2026 (2021 not conducted). Sheltered/unsheltered breakdowns by household and age type.",
        "tables": {
            "fact_pit_count": {
                "description": "One row per tract per year, with sheltered/unsheltered/street/vehicle counts broken out by household type. totpeople and totunsheltpeople are null for 2023+ (LAHSA stopped publishing these pre-computed totals that year, not a data error).",
                "columns": {
                    "tract_fips": "Census tract FIPS code",
                    "year": "PIT count year",
                    "totsheltpeople": "Total sheltered people (ES+TH+SH)",
                    "totunsheltpeople": "Total unsheltered people (null for 2023+)",
                    "totpeople": "Total PIT count, sheltered + unsheltered (null for 2023+)",
                    "totencamp": "Count of encampments",
                },
            },
            "dim_geography": {
                "description": "Tract-level geography dimension for PIT — city, SPA, council/legislative districts.",
                "columns": {},
            },
        },
    },
    "HIC (Housing Inventory Count)": {
        "description": "LAHSA's annual project-level shelter and housing bed/unit inventory, 2020-2025. Geography is SPA/CD/SD, not tract — this source has no tract-level field.",
        "tables": {
            "fact_hic": {
                "description": "One row per project per year: bed counts by household type, PIT count for that project, utilization rate, and funding-source flags.",
                "columns": {
                    "project_key": "Surrogate key (hash of org + project name) — see dim_hic_project note on confidential-provider collisions",
                    "year": "HIC year",
                    "total_beds": "Total beds at this project",
                    "pit_count": "Point-in-time count of people in this project on count night",
                    "utilization_rate": "Percentage of total beds in use on count night",
                },
            },
            "dim_hic_project": {
                "description": "Project dimension: organization, project type, geography, bed/inventory type. NOTE: LAHSA redacts org/project names to 'CONFIDENTIAL' for victim-service (DV) providers, so ~19 such projects share one project_key.",
                "columns": {},
            },
        },
    },
    "311 Encampment Requests": {
        "description": "LA City MyLA311 homeless encampment service requests, 2020-2024 (2025 excluded — source dataset stale as published). Includes lat/long, address, council district.",
        "tables": {
            "fact_311_encampment": {
                "description": "One row per 311 request.",
                "columns": {
                    "srnumber": "LA's own unique service-request ID",
                    "createddate": "When the request was filed",
                    "status": "Request status (Open/Closed)",
                    "cd": "Council district",
                },
            },
        },
    },
    "Homeless Student Enrollment (CDE)": {
        "description": "California Dept of Education data on homeless student enrollment by dwelling type, LA County, 2019-20 through 2024-25.",
        "tables": {
            "fact_homeless_students": {
                "description": "One row per county/district/school x charter_school x dass x reporting_category combination. IMPORTANT: rows overlap by design (school totals roll up into district totals). For a true unduplicated total, filter to aggregate_level='C', charter_school='All', dass='All', reporting_category='TA'.",
                "columns": {
                    "academic_year": "School year, e.g. 2023-24",
                    "aggregate_level": "C=county, D=district, S=school",
                    "district_name": "District name",
                    "homeless_student_enrollment": "Count of homeless students",
                    "temporarily_doubled_up": "Students in doubled-up housing (largest, most undercounted category)",
                    "entity_type": "school_district / county_office / sbe_charter_school — filter out non-districts for a clean district ranking",
                },
            },
        },
    },
    "City Expenses": {
        "description": "LA City homelessness-related expenditures, from the LA Controller's office.",
        "tables": {
            "fact_homelessness_expenses": {"description": "One row per expense transaction.", "columns": {}},
            "dim_department": {"description": "City department dimension.", "columns": {}},
            "dim_fund": {"description": "Funding source dimension.", "columns": {}},
            "dim_vendor": {"description": "Vendor dimension.", "columns": {}},
            "dim_project": {"description": "Project dimension.", "columns": {}},
        },
    },
}

# Flat set of every queryable table name, derived from the structure above
# so there's one source of truth rather than a separately-maintained list.
ALLOWED_TABLES = {
    table_name
    for source in TABLE_METADATA.values()
    for table_name in source["tables"]
}


# --- Natural-language to SQL ------------------------------------------------
# Reuses TABLE_METADATA (the same source that powers /schema) as the schema
# context an LLM needs to write accurate SQL — no separate metadata to
# maintain. The prompt asks Claude to classify the question first (data vs.
# platform/meta vs. unrelated) and respond accordingly, so one call handles
# routing and generation together rather than two separate steps.

_anthropic_client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

PLATFORM_CONTEXT = """
Open Civic AI is an open-source data platform for researching the LA
homelessness crisis. It integrates 6 public data sources (ACS Census
demographics, LAHSA PIT Count, LAHSA Housing Inventory Count, LA City 311
encampment requests, CDE homeless student enrollment, and LA City expenses)
into a medallion-architecture data lakehouse (bronze/silver/gold layers)
built on Apache Iceberg and stored in AWS S3, cataloged via AWS Glue. The
backend is a FastAPI service that queries the data through DuckDB. Data is
ingested and orchestrated with Dagster. The frontend is built with Next.js.
""".strip()


def _build_schema_context() -> str:
    """Builds the schema context an LLM needs, using the SAME authoritative
    source /schema uses — a live DESCRIBE against each table's actual view —
    merged with whatever manual descriptions exist in TABLE_METADATA. This
    guarantees the LLM always sees the real, full column list (previously,
    this function only used the small hand-written columns dict, which was
    incomplete — e.g. only 6 of fact_homeless_students' 24 real columns had
    descriptions, causing the model to undercount/misreport fields when
    asked directly)."""
    cur = _shared_con.cursor()
    lines = []
    try:
        for source_name, source_meta in TABLE_METADATA.items():
            lines.append(f"## {source_name}")
            lines.append(source_meta["description"])
            for table_name, table_meta in source_meta["tables"].items():
                lines.append(f"### Table: {table_name}")
                lines.append(table_meta["description"])
                result = cur.sql(f"DESCRIBE {table_name}").to_df()
                for _, row in result.iterrows():
                    col = row["column_name"]
                    col_type = row["column_type"]
                    desc = table_meta["columns"].get(col, "")
                    if desc:
                        lines.append(f"- {col} ({col_type}): {desc}")
                    else:
                        lines.append(f"- {col} ({col_type})")
            lines.append("")
    finally:
        cur.close()
    return "\n".join(lines)


# Built lazily at startup (after _shared_con exists), not at module import
# time — see _on_startup(). Declared here as a placeholder; _on_startup()
# overwrites it with the real, live schema once the database connection
# is ready.
_SCHEMA_CONTEXT = "(schema context not yet loaded)"


def _generate_sql_system_prompt() -> str:
    # A function, not a frozen f-string constant, so it always reflects the
    # current value of _SCHEMA_CONTEXT — critical since that value is
    # rebuilt at startup, after this module's top-level code has already run.
    return f"""You write DuckDB SQL for a homelessness data platform, using only the schema below. Table names are plain (no schema prefix) — e.g. SELECT * FROM fact_hic.

{_SCHEMA_CONTEXT}

Rules:
- Only write SQL for questions that require actually querying/aggregating the data itself (counts, sums, filters, rankings). Never query information_schema or any system/metadata table.
- If the question is about what tables/columns exist or what they mean (e.g. "what columns are in fact_hic", "what does totpeople mean") — that's already answered by the schema above, not something to query. Respond with exactly: NOT_A_DATA_QUESTION
- If the question is about the platform itself (architecture, tech stack, data sources) rather than the data, respond with exactly: NOT_A_DATA_QUESTION
- If the question is unrelated to this platform or its data entirely, respond with exactly: NOT_A_DATA_QUESTION
- If the question can be answered with a SQL query against this schema, respond with ONLY the SQL query, nothing else — no explanation, no markdown code fences.
- Only write SELECT statements. Never write INSERT, UPDATE, DELETE, DROP, or any other statement type.
"""


class NLQuestionRequest(BaseModel):
    question: str


@app.post("/generate-sql")
def generate_sql(request: NLQuestionRequest):
    try:
        response = _anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            system=_generate_sql_system_prompt(),
            messages=[{"role": "user", "content": request.question}],
        )
        result = response.content[0].text.strip()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not generate SQL: {e}")

    if result == "NOT_A_DATA_QUESTION":
        return {
            "sql": None,
            "message": "This looks like a question about the platform itself, or something outside this data — please try \"Ask About the Platform\" instead.",
        }
    return {"sql": _strip_markdown_fence(result), "message": None}


# --- "Get answer" — one question box, three possible outcomes --------------
# Unlike /generate-sql (which is deliberately data-question-only, feeding
# the SQL editor), this endpoint is the universal fallback: it classifies
# the question into one of three paths and always returns a plain-English
# answer, never raw SQL for the user to run themselves.
#
#   DATA:<sql>      -> run the query server-side, then a second Claude call
#                       summarizes the actual result into a sentence
#   PLATFORM:<text> -> answered directly from PLATFORM_CONTEXT, no query run
#   DECLINE:<text>  -> politely explains the question is out of scope
#
# Classification and (for data questions) SQL generation happen in one
# call; only data questions need the second summarization call, so
# platform/decline questions stay cheap — a single Haiku call.

def _answer_system_prompt() -> str:
    # A function, not a frozen f-string constant, for the same reason as
    # _generate_sql_system_prompt() above — must reflect the current,
    # startup-rebuilt value of _SCHEMA_CONTEXT.
    return f"""You answer questions about a homelessness data platform. You have two kinds of knowledge: the data schema below, and facts about the platform itself.

DATA SCHEMA:
{_SCHEMA_CONTEXT}

PLATFORM FACTS:
{PLATFORM_CONTEXT}

Classify the user's question and respond in exactly one of these three formats, with no other text:

1. If it's a question the DATA SCHEMA can answer, respond with:
DATA:<a single valid DuckDB SELECT query, table names plain with no schema prefix>

2. If it's a question about the platform itself (architecture, tech stack, how it works), OR a question about what tables/columns exist and what they mean (schema/data-dictionary questions — e.g. "what columns are in fact_hic", "what does totpeople mean", "what tables are available", "how many fields does X have"), answer using the DATA SCHEMA and PLATFORM FACTS given above. When asked to list or count columns/fields, use the exact column list given for that table in the DATA SCHEMA above — do not omit any:
PLATFORM:<a short, direct plain-English answer using only the facts given above>

3. If the question is unrelated to this platform or its data, respond with:
DECLINE:<a brief, polite sentence explaining you can only answer questions about this homelessness data platform>

Only ever write SELECT statements in the DATA case, and only for questions that require actually querying/aggregating the data itself (counts, sums, filters, rankings) — never to look up table or column structure, which is already given to you above in the DATA SCHEMA section. Never write INSERT, UPDATE, DELETE, DROP, or any other statement type, and never query information_schema or any system/metadata table.

Writing style for PLATFORM and DECLINE answers: write in clear, natural, professional English, the way a knowledgeable analyst would explain something to a colleague. Avoid awkward phrasing like "the platform use" — proofread the sentence in your head before answering. Keep it concise; a sentence or two is usually enough.
"""

SUMMARIZE_SYSTEM_PROMPT = """You are given a user's question and the results of a SQL query that answered it. Write a short, direct plain-English answer to the question using only this data. Do not mention SQL, tables, or columns — just answer naturally, like a knowledgeable person would. If the results are empty, say so plainly."""

class AnswerResponse(BaseModel):
    answer: str
    sql: str | None = None
    rows: list | None = None


@app.post("/answer", response_model=AnswerResponse)
def answer_question(request: NLQuestionRequest):
    try:
        classify_response = _anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            system=_answer_system_prompt(),
            messages=[{"role": "user", "content": request.question}],
        )
        classified = classify_response.content[0].text.strip()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not classify question: {e}")

    if classified.startswith("PLATFORM:"):
        return AnswerResponse(answer=classified[len("PLATFORM:"):].strip())

    if classified.startswith("DECLINE:"):
        return AnswerResponse(answer=classified[len("DECLINE:"):].strip())

    if classified.startswith("DATA:"):
        generated_sql = _strip_markdown_fence(classified[len("DATA:"):].strip())

        try:
            cleaned_sql = _validate_select_only(generated_sql)
        except HTTPException as e:
            # The model wrote something that failed our own safety checks —
            # fail honestly rather than silently degrading.
            raise HTTPException(
                status_code=500,
                detail=f"Generated query failed validation: {e.detail}",
            )

        cur = _shared_con.cursor()
        try:
            df = _execute_capped(cur, cleaned_sql, max_rows=20)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")
        finally:
            cur.close()

        if df is None or len(df) == 0:
            rows = []
        else:
            df = df.replace({np.nan: None})
            rows = df.to_dict(orient="records")

        try:
            summary_response = _anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                system=SUMMARIZE_SYSTEM_PROMPT,
                messages=[{
                    "role": "user",
                    "content": f"Question: {request.question}\n\nQuery results: {rows}",
                }],
            )
            answer_text = summary_response.content[0].text.strip()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not summarize results: {e}")

        return AnswerResponse(answer=answer_text, sql=cleaned_sql, rows=rows)

    # Model didn't follow the format — fail honestly rather than guess.
    raise HTTPException(
        status_code=500,
        detail=f"Could not classify the question (unexpected response format).",
    )


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

    if limit > 20000:
        limit = 20000  # basic guardrail — real pagination comes later

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


@app.get("/schema")
def get_schema():
    """Returns every queryable table, grouped by data source, with authored
    descriptions plus live column name/type read off the actual views set
    up at startup — so column names/types always reflect the real current
    schema even though the descriptions themselves are hand-maintained."""
    cur = _shared_con.cursor()
    try:
        groups = []
        for source_name, source_meta in TABLE_METADATA.items():
            tables = []
            for table_name, table_meta in source_meta["tables"].items():
                result = cur.sql(f"DESCRIBE {table_name}").to_df()
                columns = [
                    {
                        "column": row["column_name"],
                        "type": row["column_type"],
                        "description": table_meta["columns"].get(row["column_name"], ""),
                    }
                    for _, row in result.iterrows()
                ]
                tables.append(
                    {
                        "name": table_name,
                        "description": table_meta["description"],
                        "columns": columns,
                    }
                )
            groups.append(
                {
                    "source": source_name,
                    "description": source_meta["description"],
                    "tables": tables,
                }
            )
    finally:
        cur.close()
    return {"groups": groups}


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
    chain = "env" if os.environ.get("LAKEHOUSE_ENV") == "aws" else "config"
    con.sql(f"CREATE SECRET IF NOT EXISTS (TYPE s3, PROVIDER credential_chain, CHAIN '{chain}');")
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
    global _SCHEMA_CONTEXT
    _init_shared_connection()
    # Now that _shared_con exists, rebuild the schema context from the real,
    # live table structure — this is what fixes the earlier bug where the
    # LLM only saw a small, hand-written subset of each table's columns.
    _SCHEMA_CONTEXT = _build_schema_context()


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

        total_count = None
        if row_count == MAX_ROWS:
            count_cur = _shared_con.cursor()
            try:
                count_result = count_cur.sql(f"SELECT COUNT(*) AS n FROM ({cleaned_sql}) AS user_query")
                total_count = int(count_result.to_df()["n"].iloc[0])
            finally:
                count_cur.close()

        return {
            "row_count": row_count,
            "truncated": row_count == MAX_ROWS,
            "total_count": total_count,
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

@app.post("/query/export")
def export_query(request: QueryRequest):
    cleaned_sql = _validate_select_only(request.sql)

    cur = _shared_con.cursor()
    try:
        result = cur.sql(cleaned_sql)
        df = result.to_df() if result is not None else None
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query failed: {e}")
    finally:
        cur.close()

    if df is None or len(df) == 0:
        raise HTTPException(status_code=404, detail="Query returned no rows.")

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=query_export.csv"},
    )