from pyspark.sql import SparkSession
from src.spark.utils.config import load_config

def run(config_path: str):
    config = load_config(config_path)
    spark = SparkSession.builder.appName("enrich_pit").getOrCreate()

    silver_path = config["paths"]["silver"] + "/pit_clean"
    gold_path = config["paths"]["gold"] + "/pit_metrics"

    df = spark.read.format("delta").load(silver_path)

    df_metrics = (
        df.groupBy("year", "spa")
          .sum("count")
          .withColumnRenamed("sum(count)", "total_count")
    )

    df_metrics.write.format("delta").mode("overwrite").save(gold_path)
    spark.stop()
