# lh_orch/definitions.py

from dotenv import load_dotenv
load_dotenv(override=True)

import dagster as dg

from lh_orch.lh_assets.bronze_expenses import bronze_expenses
from lh_orch.lh_assets.silver_expenses import silver_expenses
from lh_orch.lh_assets.gold_expenses import gold_expenses

from lh_orch.lh_assets.counts import bronze_count, silver_count, gold_count
from lh_orch.lh_assets.lacity_jobs import lacity_ingestion_job
from lh_orch.lh_assets.lacity_sensor import lacity_sensor
from lh_orch.lh_assets.bronze_acs import bronze_acs_estimates
from lh_orch.lh_assets.silver_acs import silver_acs_estimates

bronze_silver_daily_schedule = dg.ScheduleDefinition(
    job=lacity_ingestion_job,
    cron_schedule="0 2 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        bronze_expenses,
        silver_expenses,
        gold_expenses,
        bronze_count,
        silver_count,
        gold_count,
        bronze_acs_estimates,
        silver_acs_estimates,
    ],
    jobs=[lacity_ingestion_job],
    schedules=[bronze_silver_daily_schedule],
    sensors=[lacity_sensor],
)
