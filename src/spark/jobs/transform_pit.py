from pyspark.sql import SparkSession
from src.spark.utils.config import load_config

def run(config_path: str):
    config = load_config(config_path)
    spark = SparkSession.builder.appName("transform_pit").getOrCreate()

    bronze_path = config["paths"]["bronze"] + "/pit_delta"
    silver_path = config["paths"]["silver"] + "/pit_clean"

    df = spark.read.format("delta").load(bronze_path)

    df_clean = (
        df.withColumn("spa", df["spa"].cast("string"))
          .withColumn("population_type", df["population_type"].cast("string"))
          .withColumn("count", df["count"].cast("int"))
    )

    df_clean.write.format("delta").mode("overwrite").save(silver_path)
    spark.stop()
