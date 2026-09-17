from pathlib import Path
from pyspark.sql import SparkSession

BRONZE_PATH = Path("data/bronze/paysim")
spark = (
    SparkSession.builder
    .appName("PaySimBronzeValidation")
    .getOrCreate()
)

df = spark.read.parquet(str(BRONZE_PATH))

print("Bronze schema:")
df.printSchema()

print("\nBronze rows:", df.count())

print("\nSamples:")
df.show(5, truncate=False)

spark.stop()
