from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = Path("data/silver/paysim")
REJECTED_PATH = ("data/silver/rejected")

spark =(
    SparkSession.builder.appName("PaySimSilverValidation")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

# Read persisted Silver datasets
df_silver = spark.read.parquet(str(SILVER_PATH))
df_rejected = spark.read.parquet(str(REJECTED_PATH))


# Validate record counts
silver_count = df_silver.count()
rejected_count = df_rejected.count()

print("\nSilver rows:", silver_count)
print("Rejected rows:", rejected_count)

# Validate derived time fields
invalid_day_count = (
    df_silver
    .filter(F.col("transaction_day") < 1)
    .count()
)

invalid_hour_count = (
    df_silver
    .filter(
        (F.col("transaction_hour") < 0)
        | (F.col("transaction_hour") > 23)
    )
    .count()
)

print("Invalid transaction_day rows:", invalid_day_count)
print("Invalid transaction_hour rows:", invalid_hour_count)

# Validate that internal control columns were removed
internal_columns = [
    "is_valid",
    "rejected_reason",
]

unexpected_internal_columns = [
    column
    for column in internal_columns
    if column in df_silver.columns
]

print(
    "Unexpected internal columns:",
    unexpected_internal_columns
)

# Show final Silver schema and samples
print("\nSilver Schema:")
df_silver.printSchema()

print("\nSilver sample:")
df_silver.select(
    "step",
    "transaction_day",
    "transaction_hour",
    "type",
    "amount",
    "isFraud",
).show(10, truncate=False)

spark.stop()