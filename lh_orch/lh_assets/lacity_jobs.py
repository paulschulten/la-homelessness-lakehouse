# lh_orch/lh_assets/lacity_jobs.py

import dagster as dg

from lh_orch.lh_assets.bronze_expenses import bronze_expenses
from lh_orch.lh_assets.silver_expenses import silver_expenses
from lh_orch.lh_assets.gold_expenses import gold_expenses
from lh_orch.lh_assets.counts import bronze_count, silver_count, gold_count

lacity_ingestion_job = dg.define_asset_job(
    name="lacity_ingestion_job",
    selection=[
        bronze_expenses,
        silver_expenses,
        gold_expenses,
        bronze_count,
        silver_count,
        gold_count,
    ],
)