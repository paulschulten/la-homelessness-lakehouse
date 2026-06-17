from src.spark.jobs.ingest_pit import run as ingest_run
from src.spark.jobs.transform_pit import run as transform_run
from src.spark.jobs.enrich_pit import run as enrich_run

def run_pipeline(config_path: str):
    print("Starting PIT pipeline...")

    print("Step 1: Ingesting Bronze data")
    ingest_run(config_path)

    print("Step 2: Transforming to Silver")
    transform_run(config_path)

    print("Step 3: Enriching to Gold")
    enrich_run(config_path)

    print("Pipeline complete.")
