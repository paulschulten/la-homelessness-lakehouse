# lh_orch/definitions.py

import dagster as dg

from lh_orch.lh_assets.bronze_expenses import bronze_expenses
from lh_orch.lh_assets.silver_expenses import silver_expenses
from lh_orch.lh_assets.gold_expenses import gold_expenses

from lh_orch.lh_assets.counts import raw_count, silver_count, gold_count
from lh_orch.lh_assets.lacity_sensor import lacity_sensor, lacity_ingestion_job

bronze_silver_daily_schedule = dg.ScheduleDefinition(
    job=lacity_ingestion_job,
    cron_schedule="0 2 * * *",
)

defs = dg.Definitions(
    assets=[
        bronze_expenses,
        silver_expenses,
        gold_expenses,
        raw_count,
        silver_count,
        gold_count,
    ],
    jobs=[lacity_ingestion_job],
    schedules=[bronze_silver_daily_schedule],
    sensors=[lacity_sensor],
)
