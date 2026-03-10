from pyspark.sql import SparkSession

# Création de la SparkSession avec Delta activé
spark = SparkSession.builder \
    .appName("Bronze Layer Test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Création d’un DataFrame test
df = spark.range(5)

# Écriture en format Delta dans le volume
df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/data/bronze/test_delta")

print("Delta table written successfully!")

spark.stop()