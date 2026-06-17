from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def pit_schema() -> StructType:
    """Schema for PIT (Point-in-Time) count data."""
    return StructType([
        StructField("year", IntegerType(), True),
        StructField("spa", StringType(), True),
        StructField("population_type", StringType(), True),
        StructField("count", IntegerType(), True)
    ])
