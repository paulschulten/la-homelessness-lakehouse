# lh_orch/lh_assets/lacity_sensor.py

import requests
import dagster as dg

from lh_orch.lh_assets.counts import bronze_count
from lh_orch.lh_assets.lacity_jobs import lacity_ingestion_job

# Correct endpoint based on your curl output
SOCRATA_URL = "https://controllerdata.lacity.org/resource/98ve-cuf5.json?$select=count(*)"

@dg.sensor(job=lacity_ingestion_job)
def lacity_sensor(context: dg.SensorEvaluationContext):
    """
    Triggers ingestion when the LA Controller site record count changes.
    """

    # 1. Fetch record count from LA Controller API
    try:
        response = requests.get(SOCRATA_URL, timeout=10)
        response.raise_for_status()

        data = response.json()
        context.log.info(f"Raw API response: {data}")

        controller_count = int(data[0]["count"])

    except Exception as e:
        context.log.error(f"Failed to fetch LA site count: {e}")
        return dg.SkipReason(f"Failed to fetch LA site count: {e}")

    # 2. Fetch Dagster's last materialized bronze_count
    try:
        raw_event = context.instance.get_latest_materialization_event(bronze_count.key)
        bronze_count_value = raw_event.materialization.metadata["records"].value
    except Exception:
        bronze_count_value = -1  # force run if never materialized

    context.log.info(f"Controller site count: {controller_count}")
    context.log.info(f"Dagster bronze_count: {bronze_count_value}")

    # 3. Compare counts — trigger ingestion only when they differ
    if controller_count != bronze_count_value:
        context.log.info("Change detected — triggering ingestion pipeline.")
        return dg.RunRequest(run_key=str(controller_count))

    return dg.SkipReason("No change detected — skipping run.")