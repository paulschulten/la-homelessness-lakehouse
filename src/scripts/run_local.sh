#!/bin/bash

CONFIG_PATH="env/spark_config_local.yaml"

echo "Running PIT pipeline locally..."
python3 -c "from spark.pipelines.pit_pipeline import run_pipeline; run_pipeline('$CONFIG_PATH')"
