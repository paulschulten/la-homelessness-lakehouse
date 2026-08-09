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
from lh_orch.lh_assets.bronze_hic import bronze_hic
from lh_orch.lh_assets.silver_hic import silver_hic
from lh_orch.lh_assets.gold_hic import gold_dim_hic_project, gold_fact_hic
from lh_orch.lh_assets.bronze_311 import bronze_311_encampment
from lh_orch.lh_assets.silver_311 import silver_311_encampment
from lh_orch.lh_assets.gold_311 import gold_fact_311_encampment
from lh_orch.lh_assets.bronze_homeless_students import bronze_homeless_students
from lh_orch.lh_assets.silver_homeless_students import silver_homeless_students
from lh_orch.lh_assets.gold_homeless_students import gold_fact_homeless_students

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
        bronze_hic,
        bronze_311_encampment,
        bronze_homeless_students,
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
        gold_dim_hic_project,
        gold_fact_hic,
        gold_fact_311_encampment,
        gold_fact_homeless_students,
        silver_count,
        silver_expenses,
        silver_acs_estimates,
        silver_pit_count,
        silver_tiger_tracts,
        silver_hic,
        silver_311_encampment,
        silver_homeless_students,
    ],
    jobs=[lacity_ingestion_job],
    schedules=[bronze_silver_daily_schedule],
    sensors=[lacity_sensor],
)
