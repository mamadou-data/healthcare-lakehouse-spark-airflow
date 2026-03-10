from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, quarter, dayofweek, date_format

spark = (
    SparkSession.builder
    .appName("Gold Emergency Room Modeling")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

silver_path = "/opt/spark/data/silver/emergency_room"
df = spark.read.format("delta").load(silver_path)

# -----------------------------
# Dimension Patient
# -----------------------------
dim_patient = df.select(
    "patient_id",
    "patient_first_inital",
    "patient_last_name",
    "patient_gender",
    "age",
    "age_group",
    "patient_race"
).dropDuplicates()

dim_patient.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/gold/dim_patient")

# -----------------------------
# Dimension Department
# -----------------------------
dim_department = df.select("department_referral") \
    .dropDuplicates()

dim_department.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/gold/dim_department")

# -----------------------------
# Dimension Date
# -----------------------------
dim_date = df.select("admission_timestamp") \
    .withColumn("year", year(col("admission_timestamp"))) \
    .withColumn("month", month(col("admission_timestamp"))) \
    .withColumn("month_name", date_format(col("admission_timestamp"), "MMMM")) \
    .withColumn("quarter", quarter(col("admission_timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("admission_timestamp"))) \
    .dropDuplicates()

dim_date.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/gold/dim_date")

# -----------------------------
# Fact Table
# -----------------------------
fact = df.select(
    "patient_id",
    "department_referral",
    "admission_timestamp",
    "wait_time_minutes",
    "satisfaction_score",
    "admission_flag",
    "patients_cm"
)

fact.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/gold/fact_emergency_visit")

print("✅ Gold Layer created successfully.")

spark.stop()