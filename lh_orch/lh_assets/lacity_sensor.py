# lh_orch/lh_assets/lacity_sensor.py

import requests
from dagster import sensor, RunRequest
from lh_orch.lh_assets.counts import raw_count

SOCRATA_URL = "https://controllerdata.lacity.org/resource/2nrs-mtv8.json?$select=count(*)"

@sensor
def lacity_sensor(context):
    """
    Triggers a pipeline run when the LA Controller site record count changes.
    Compares the Socrata count to the raw_count asset stored in Dagster.
    """

    # 1. Fetch record count from LA Controller site
    try:
        response = requests.get(SOCRATA_URL, timeout=10)
        response.raise_for_status()
        controller_count = int(response.json()[0]["count"])
    except Exception as e:
        context.log.error(f"Failed to fetch LA site count: {e}")
        return None

    # 2. Fetch Dagster's last materialized raw_count
    try:
        raw_event = context.instance.get_latest_materialization_event(raw_count.key)
        raw_count_value = raw_event.materialization.metadata["records"].value
    except Exception:
        # If raw_count has never been materialized, force a run
        raw_count_value = -1

    context.log.info(f"Controller site count: {controller_count}")
    context.log.info(f"Dagster raw_count: {raw_count_value}")

    # 3. Compare counts — trigger ingestion only when they differ
    if controller_count != raw_count_value:
        context.log.info("Change detected — triggering ingestion pipeline.")
        return RunRequest(run_key=str(controller_count))

    # No change → no run
    return None
