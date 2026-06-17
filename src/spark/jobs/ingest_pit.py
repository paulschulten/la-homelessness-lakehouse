from pyspark.sql import SparkSession
from spark.utils.io import read_csv, write_delta
from src.spark.utils.schema import pit_schema
from src.spark.utils.config import load_config

def run(config_path: str):
    config = load_config(config_path)
    spark = SparkSession.builder.appName("ingest_pit").getOrCreate()

    input_path = config["paths"]["bronze"] + "/pit_raw.csv"
    output_path = config["paths"]["bronze"] + "/pit_delta"

    df = read_csv(spark, input_path)
    df = spark.createDataFrame(df.rdd, pit_schema())

    write_delta(df, output_path)
    spark.stop()
