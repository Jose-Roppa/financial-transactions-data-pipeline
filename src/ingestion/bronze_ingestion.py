from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, DoubleType)

RAW_PATH = Path("data/raw/PS_20174392719_1491204439457_log.csv")
BRONZE_PATH = Path("data/bronze/paysim")

spark = (
    SparkSession.builder
    .appName("PaySimBronzeIngestion")
    .getOrCreate()
)

schema = StructType([
    StructField("step", IntegerType(), False),
    StructField("type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("nameOrig", StringType(), False),
    StructField("oldbalanceOrg", DoubleType(), False),
    StructField("newbalanceOrig", DoubleType(), False),
    StructField("nameDest", StringType(), False),
    StructField("oldbalanceDest", DoubleType(), False),
    StructField("newbalanceDest", DoubleType(), False),
    StructField("isFraud", IntegerType(), False),
    StructField("isFlaggedFraud", IntegerType(), False),
])

df = (
    spark.read.option("header", True).schema(schema).csv(str(RAW_PATH))
)

df.printSchema()
df.show(5, truncate=False)

print("Rows: ", df.count())

(
    df.write.mode("overwrite").parquet(str(BRONZE_PATH))
)

spark.stop()