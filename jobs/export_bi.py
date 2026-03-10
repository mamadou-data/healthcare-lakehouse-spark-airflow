from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

gold_base = "/opt/spark/data/gold"
bi_base = "/opt/spark/data/bi_export"

tables = [
    "dim_patient",
    "dim_department",
    "dim_date",
    "fact_emergency_visit"
]

for table in tables:
    df = spark.read.format("delta").load(f"{gold_base}/{table}")
    
    df.write \
        .mode("overwrite") \
        .parquet(f"{bi_base}/{table}")

print("✅ BI Export Completed")

spark.stop()