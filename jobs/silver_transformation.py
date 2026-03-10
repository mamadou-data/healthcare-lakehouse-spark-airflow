from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, coalesce, lit

# -----------------------------
# 1. Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("Silver Emergency Room Transformation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -----------------------------
# 2. Read Bronze
# -----------------------------
bronze_path = "/opt/spark/data/bronze/emergency_room"
df = spark.read.format("delta").load(bronze_path)

# -----------------------------
# 3. Cast & Rename Business Fields
# -----------------------------
df_silver = (
    df
    .withColumn("admission_timestamp",to_timestamp(col("patient_admission_date"), "dd-MM-yyyy HH:mm"))
    .withColumn("age", col("patient_age").cast("int"))
    .withColumn("wait_time_minutes", col("patient_waittime").cast("int"))
    .withColumn("satisfaction_score", col("patient_satisfaction_score").cast("double"))
    .withColumn("admission_flag", col("patient_admission_flag").cast("boolean"))
)

# -----------------------------
# 4. Null Handling
# -----------------------------
df_silver = df_silver.withColumn(
    "department_referral",
    coalesce(col("department_referral"), lit("Unknown"))
)

# -----------------------------
# 5. Business Enrichment
# -----------------------------
df_silver = df_silver.withColumn(
    "age_group",
    when(col("age") <= 17, "Minor")
    .when((col("age") <= 40), "Adult")
    .when((col("age") <= 65), "Middle_Age")
    .otherwise("Senior")
)

df_silver = df_silver.withColumn(
    "wait_time_bucket",
    when(col("wait_time_minutes") <= 15, "0_15")
    .when(col("wait_time_minutes") <= 30, "16_30")
    .when(col("wait_time_minutes") <= 60, "31_60")
    .otherwise("60_plus")
)

# -----------------------------
# 6. Select final schema
# -----------------------------
df_silver = df_silver.select(
    "patient_id",
    "admission_timestamp",
    "patient_first_inital",
    "patient_last_name",
    "patient_gender",
    "age",
    "age_group",
    "patient_race",
    "department_referral",
    "admission_flag",
    "satisfaction_score",
    "wait_time_minutes",
    "wait_time_bucket",
    "patients_cm",
    "ingestion_timestamp"
)

# -----------------------------
# 7. Write Silver
# -----------------------------
silver_path = "/opt/spark/data/silver/emergency_room"

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)

print("✅ Silver transformation completed successfully.")

spark.stop()