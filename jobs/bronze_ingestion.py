from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import re

# -----------------------------
# 1. Spark Session with Delta
# -----------------------------
spark = (
    SparkSession.builder
    .appName("Bronze Emergency Room Ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -----------------------------
# 2. Read Raw CSV
# -----------------------------
raw_path = "/opt/spark/data/raw/emergency_room.csv"

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

# -----------------------------
# 3. Clean Column Names
# -----------------------------
def clean_column_name(col_name):
    col_name = col_name.strip().lower()
    col_name = re.sub(r"[^\w]", "_", col_name)
    return col_name

df_raw = df_raw.toDF(*[clean_column_name(c) for c in df_raw.columns])

# -----------------------------
# 4. Add ingestion metadata
# -----------------------------
df_bronze = df_raw.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# -----------------------------
# 5. Write to Bronze (Delta)
# -----------------------------
bronze_path = "/opt/spark/data/bronze/emergency_room"

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .save(bronze_path)
)

print("✅ Bronze ingestion completed successfully.")

spark.stop()