import duckdb
import os
from datetime import datetime, UTC

BRONZE_DIR = "02_data/01_raw/lacity/01_homelessness_expenses"
SILVER_DIR = "02_data/02_silver/lacity/01_homelessness_expenses"

# Find latest Bronze file
bronze_files = sorted(
    [f for f in os.listdir(BRONZE_DIR) if f.endswith(".json")]
)
BRONZE_PATH = os.path.join(BRONZE_DIR, bronze_files[-1])

# Timestamped Silver output
ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
SILVER_PATH = os.path.join(
    SILVER_DIR,
    f"homelessness_expenses_{ts}.parquet"
)

ingest_ts = datetime.now(UTC).isoformat()

con = duckdb.connect()

con.execute(f"""
    CREATE OR REPLACE TABLE homelessness_expenses_silver AS
    WITH source AS (
        SELECT
            fiscal_year::VARCHAR              AS fiscal_year,
            dept_nm::VARCHAR                  AS dept_name,
            fund_nm::VARCHAR                  AS fund_nm,
            mjr_project_code::VARCHAR         AS mjr_project_code,
            mjr_project_name::VARCHAR         AS mjr_project_name,
            business_type::VARCHAR            AS business_type,
            payment_type::VARCHAR             AS payment_type,
            vendor_name::VARCHAR              AS vendor_name,
            payment_description::VARCHAR      AS payment_description,
            work_order_nm::VARCHAR            AS work_order_name,
            work_order::VARCHAR               AS work_order,
            appr_nm::VARCHAR                  AS appr_name,

            doc_id::VARCHAR                   AS doc_id,
            doc_cd::VARCHAR                   AS doc_cd,
            dept_cd::VARCHAR                  AS dept_cd,
            appr_cd::VARCHAR                  AS appr_cd,
            doc_actg_ln_no::VARCHAR           AS doc_actg_ln_no,
            fund_cd::VARCHAR                  AS fund_cd,
            transaction_closed::VARCHAR       AS transaction_closed,
            vend_cust_cd::VARCHAR             AS vend_cust_cd,

            CAST(pstng_am AS DOUBLE)          AS amount,
            STRPTIME(transaction_date, '%m/%d/%Y') AS transaction_date,

            TIMESTAMP '{ingest_ts}'           AS ingest_timestamp
        FROM read_json_auto('{BRONZE_PATH}')
    ),
    deduped AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    doc_id,
                    doc_cd,
                    dept_cd,
                    vend_cust_cd,
                    fiscal_year,
                    doc_actg_ln_no
                ORDER BY ingest_timestamp DESC
            ) AS rn
        FROM source
    )
    SELECT *
    FROM deduped
    WHERE rn = 1
""")

con.execute(f"""
    COPY homelessness_expenses_silver
    TO '{SILVER_PATH}'
    (FORMAT PARQUET, OVERWRITE TRUE)
""")

print(f"Silver file written: {SILVER_PATH}")
