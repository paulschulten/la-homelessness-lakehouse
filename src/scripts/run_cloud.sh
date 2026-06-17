#!/bin/bash

CONFIG_PATH="env/spark_config_cloud.yaml"

echo "Running PIT pipeline on cloud cluster..."
python3 -c "from spark.pipelines.pit_pipeline import run_pipeline; run_pipeline('$CONFIG_PATH')"
