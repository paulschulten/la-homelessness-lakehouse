# lh_orch/definitions.py

from dotenv import load_dotenv
load_dotenv(override=True)

import dagster as dg

from lh_orch.lh_assets.bronze_expenses import bronze_expenses
from lh_orch.lh_assets.silver_expenses import silver_expenses
from lh_orch.lh_assets.gold_expenses import gold_fact_expenses, gold_dim_department, gold_dim_fund, gold_dim_vendor, gold_dim_project
from lh_orch.lh_assets.counts import bronze_count, silver_count, gold_count
from lh_orch.lh_assets.lacity_jobs import lacity_ingestion_job
from lh_orch.lh_assets.lacity_sensor import lacity_sensor
from lh_orch.lh_assets.bronze_acs import bronze_acs_estimates
from lh_orch.lh_assets.silver_acs import silver_acs_estimates
from lh_orch.lh_assets.gold_acs import gold_dim_variable, gold_dim_tract, gold_fact_acs_estimates
from lh_orch.lh_assets.bronze_pit import bronze_pit_count
from lh_orch.lh_assets.silver_pit import silver_pit_count
from lh_orch.lh_assets.gold_pit import gold_fact_pit_count, gold_dim_geography
from lh_orch.lh_assets.bronze_tiger import bronze_tiger_tracts
from lh_orch.lh_assets.silver_tiger import silver_tiger_tracts
from lh_orch.lh_assets.gold_geography import gold_dim_tiger_geography

bronze_silver_daily_schedule = dg.ScheduleDefinition(
    job=lacity_ingestion_job,
    cron_schedule="0 2 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        bronze_expenses,
        bronze_count,
        bronze_acs_estimates,
        bronze_pit_count,
        bronze_tiger_tracts,
        gold_fact_expenses,
        gold_dim_department,
        gold_dim_fund,
        gold_dim_vendor,
        gold_dim_project,
        gold_count,
        gold_dim_variable,
        gold_dim_tract,
        gold_fact_acs_estimates,
        gold_fact_pit_count,
        gold_dim_tiger_geography,
        silver_count,
        silver_expenses,
        silver_acs_estimates,
        silver_pit_count,
        silver_tiger_tracts,
    ],
    jobs=[lacity_ingestion_job],
    schedules=[bronze_silver_daily_schedule],
    sensors=[lacity_sensor],
)
