import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def validate_gold_layer():
    spark = SparkSession.builder.appName("ValidateGoldLayer").getOrCreate()

    print("\n=== STEP 1: Load Gold Tables ===")
    gb = spark.read.parquet("gold/lacity_business")
    gg = spark.read.parquet("gold/lacity_geography")
    gd = spark.read.parquet("gold/lacity_business_by_district")

    print(f"Gold_Business rows: {gb.count()}")
    print(f"Gold_Geography rows: {gg.count()}")
    print(f"Gold_Business_By_District rows: {gd.count()}")

    print("\n=== STEP 2: Validate Row Count Consistency ===")
    total_business = gb.count()
    district_sum = gd.agg(F.sum("business_count")).collect()[0][0]
    print(f"Total businesses: {total_business}")
    print(f"Sum of district business_count: {district_sum}")
    print("PASS" if total_business == district_sum else "FAIL")

    print("\n=== STEP 3: Geography Integrity ===")
    distinct_geo = gg.select("latitude", "longitude").distinct().count()
    null_lat = gg.filter(F.col("latitude").isNull()).count()
    null_lon = gg.filter(F.col("longitude").isNull()).count()
    print(f"Distinct lat/lon pairs: {distinct_geo}")
    print(f"Null latitude: {null_lat}, Null longitude: {null_lon}")

    print("\n=== STEP 4: District Coverage ===")
    districts = [r[0] for r in gd.select("council_district").distinct().orderBy("council_district").collect()]
    print("Districts:", districts)

    print("\n=== STEP 5: NAICS Integrity ===")
    null_naics = gb.filter(F.col("naics").isNull()).count()
    distinct_naics = gb.select("naics").distinct().count()
    print(f"Null NAICS: {null_naics}")
    print(f"Distinct NAICS: {distinct_naics}")

    print("\n=== STEP 6: Schema Validation ===")
    print("Gold_Business schema:")
    gb.printSchema()
    print("Gold_Geography schema:")
    gg.printSchema()
    print("Gold_Business_By_District schema:")
    gd.printSchema()

    print("\n=== VALIDATION COMPLETE ===")
    spark.stop()


if __name__ == "__main__":
    validate_gold_layer()
