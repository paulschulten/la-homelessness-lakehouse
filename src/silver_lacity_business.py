from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# Raw business dataset (correct filename)

import json

raw_text = "".join([row[0] for row in spark.read.text("data/raw/lacity_business.json").collect()])
records = json.loads(raw_text)

# Convert list of dicts into Spark DataFrame
df_raw = spark.createDataFrame(records)

df = (
    df_raw
    # Flatten nested location_1
    .withColumn("latitude", F.col("location_1.latitude").cast("double"))
    .withColumn("longitude", F.col("location_1.longitude").cast("double"))
    
    # Clean string fields
    .withColumn("location_account", F.trim(F.col("location_account")))
    .withColumn("business_name", F.trim(F.col("business_name")))
    .withColumn("street_address", F.trim(F.col("street_address")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("zip_code", F.trim(F.col("zip_code")))
    .withColumn("location_description", F.trim(F.col("location_description")))
    .withColumn("primary_naics_description", F.trim(F.col("primary_naics_description")))
    
    # Cast numeric + date fields
    .withColumn("naics", F.col("naics").cast("int"))
    .withColumn("council_district", F.col("council_district").cast("int"))
    .withColumn("location_start_date", F.to_timestamp("location_start_date"))
    
    # Drop nested + Socrata metadata fields
    .drop(
        "location_1",
        "@computed_region_qz3q_ghft",
        "@computed_region_kqwf_mjcx",
        "@computed_region_k96s_3jcv",
        "@computed_region_tatf_ua23",
        "@computed_region_2dna_qi2s",
    )
)

df.write.mode("overwrite").parquet("silver/lacity_business")
