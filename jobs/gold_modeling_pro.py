from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, quarter, dayofweek, date_format
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = (
    SparkSession.builder
    .appName("Gold PRO Clean")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

silver_path = "/opt/spark/data/silver/emergency_room"
df = spark.read.format("delta").load(silver_path)

# =============================
# DIM PATIENT
# =============================
window_patient = Window.orderBy("patient_id")

dim_patient = df.select(
    "patient_id",
    "patient_first_inital",
    "patient_last_name",
    "patient_gender",
    "age",
    "age_group",
    "patient_race"
).dropDuplicates()

dim_patient = dim_patient.withColumn(
    "patient_key",
    row_number().over(window_patient)
)

dim_patient.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/gold/dim_patient")

# =============================
# DIM DEPARTMENT
# =============================
window_dep = Window.orderBy("department_referral")

dim_department = df.select("department_referral") \
    .dropDuplicates()

dim_department = dim_department.withColumn(
    "department_key",
    row_number().over(window_dep)
)

dim_department.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/gold/dim_department")

# =============================
# DIM DATE
# =============================
dim_date = df.withColumn("date", col("admission_timestamp").cast("date")) \
    .select("date") \
    .dropDuplicates()

dim_date = dim_date \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("day_of_week", dayofweek(col("date"))) \
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))

dim_date.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/gold/dim_date")

df.select("admission_timestamp").show(5, False)

# =============================
# FACT
# =============================
fact = df \
    .withColumn("date_key",
                date_format(col("admission_timestamp"), "yyyyMMdd").cast("int")) \
    .join(dim_patient.select("patient_id", "patient_key"), "patient_id") \
    .join(dim_department.select("department_referral", "department_key"), "department_referral")

fact = fact.select(
    "patient_key",
    "department_key",
    "date_key",
    "wait_time_minutes",
    "satisfaction_score",
    "admission_flag",
    "patients_cm"
)

print("FACT ROW COUNT:", fact.count())

fact.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date_key") \
    .save("/opt/spark/data/gold/fact_emergency_visit")

print("✅ GOLD PRO COMPLETE")
print("Silver row count:", df.count())
print("FACT ROW COUNT:", fact.count())

spark.stop()