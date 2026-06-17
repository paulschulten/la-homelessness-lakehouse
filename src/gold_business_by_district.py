import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from utils.io import console

def build_gold_business_by_district(spark: SparkSession):
    console.print("[yellow]Loading Gold lacity_business...[/yellow]")

    df = spark.read.parquet("gold/lacity_business")

    console.print("[yellow]Aggregating business metrics by council district...[/yellow]")

    # Normalize column names
    df = df.toDF(*[c.lower().strip() for c in df.columns])

    # Group by council district
    agg = (
        df.groupBy("council_district")
          .agg(
              F.count("*").alias("business_count"),
              F.countDistinct("naics").alias("unique_categories"),
              F.avg("latitude").alias("avg_latitude"),
              F.avg("longitude").alias("avg_longitude")
          )
          .orderBy("council_district")
    )

    # Add metadata
    agg = (
        agg.withColumn("ingestion_date", F.current_date())
           .withColumn("data_source", F.lit("LA City Business Dataset"))
           .withColumn("data_domain", F.lit("business_by_district"))
    )

    console.print("[green]Gold_Business_By_District table created.[/green]")

    agg.write.mode("overwrite").parquet("gold/lacity_business_by_district")

    return agg


if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldBusinessByDistrict").getOrCreate()
    build_gold_business_by_district(spark)
    spark.stop()
