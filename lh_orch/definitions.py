from dagster import Definitions, ScheduleDefinition

from .lh_assets.bronze_expenses import bronze_expenses
from .lh_assets.silver_expenses import silver_expenses
from .lh_assets.gold_expenses import gold_expenses
from .lh_assets.counts import raw_count, silver_count, gold_count


bronze_silver_daily_schedule = ScheduleDefinition(
    job_name="__ASSET_JOB",
    cron_schedule="0 2 * * *",   # 2:00 AM every day
)

defs = Definitions(
    assets=[
        bronze_expenses,
        silver_expenses,
        gold_expenses,
        raw_count,
        silver_count,
        gold_count,
    ],
    schedules=[bronze_silver_daily_schedule],
    
)
