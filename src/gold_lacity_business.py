from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load Silver
silver = spark.read.parquet("silver/lacity_business")

# Build Gold_Business
gold_business = silver.select(
    "business_name",
    "location_account",
    "naics",
    "primary_naics_description",
    "council_district",
    "location_start_date",
    "latitude",
    "longitude",
    "zip_code",
    "city"
)

# Write Gold
gold_business.write.mode("overwrite").parquet("gold/business")
