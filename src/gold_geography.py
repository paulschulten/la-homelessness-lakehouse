import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from utils.io import console   # <-- correct logger

def build_gold_geography(spark: SparkSession):
    console.print("[yellow]Loading Silver lacity_business...[/yellow]")

    df = spark.read.parquet("silver/lacity_business")

    console.print("[yellow]Extracting unique geography records...[/yellow]")

    # Normalize column names
    df = df.toDF(*[c.lower().strip() for c in df.columns])

    # Select only geographic fields
    geo_cols = [
        "location_account",
        "street_address",
        "city",
        "zip_code",
        "council_district",
        "latitude",
        "longitude"
    ]

    geo = df.select(*geo_cols)

    # Deduplicate by coordinates + address
    geo = geo.dropDuplicates(["street_address", "zip_code", "latitude", "longitude"])

    # Add metadata
    geo = (
        geo.withColumn("ingestion_date", F.current_date())
           .withColumn("data_source", F.lit("LA City Business Dataset"))
           .withColumn("data_domain", F.lit("geography"))
    )

    console.print("[green]Gold_Geography table created.[/green]")

    geo.write.mode("overwrite").parquet("gold/lacity_geography")

    return geo


if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldGeography").getOrCreate()
    build_gold_geography(spark)
    spark.stop()
