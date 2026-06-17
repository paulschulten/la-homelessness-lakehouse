from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Example schema placeholder
business_schema = StructType([
    StructField("location_account", StringType(), True),
    StructField("street_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("council_district", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])
