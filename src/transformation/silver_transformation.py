from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = Path("data/bronze/paysim")
SILVER_PATH= Path("data/silver/paysim")
REJECTED = Path("data/silver/rejected")

spark = (
    SparkSession.builder
    .appName("PaySimSilverTransformation")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

df = spark.read.parquet(str(BRONZE_PATH))

print("Bronze rows:", df.count())

VALID_TRANSACTION_TYPES = [
    "CASH_OUT",
    "PAYMENT",
    "CASH_IN",
    "TRANSFER",
    "DEBIT",
]

REQUIRED_FIELDS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "nameDest",
    "isFraud",
    "isFlaggedFraud",
]

# Build validation rule for required fields
missing_required_field = F.lit(False)

for field in REQUIRED_FIELDS:
    missing_required_field = (
        missing_required_field | F.col(field).isNull() 
    )

# Validate transaction type
invalid_transaction_type = (
    ~F.col("type").isin(VALID_TRANSACTION_TYPES)
)

# validate transaction amount
invalid_amount = (
    F.col("amount").isNull()
    | (F.col("amount") < 0 )
)

# Store validation failures instead of dropping them
df_validated = df.withColumn (
    "rejection_reason",
    F.concat_ws(
        "; ",
        F.when(
            missing_required_field,
            F.lit("Missing_required_field")
        ),
        F.when(
            invalid_transaction_type,
            F.lit("Invalid_transaction_type")
        ),
        F.when(
            invalid_amount,
            F.lit("Invalid_amount")
        ),
    )
)

df_validated = df_validated.withColumn(
    "is_valid",
    F.col("rejection_reason") == ""
)

print("\nValidation summary:")

df_validated.groupBy("is_valid").count().show()

print("\nRejection reasons:")

df_validated.groupBy("rejection_reason").count().show(
    truncate=False
)

# Separate valid and rejected records
df_valid = df_validated.filter(
    F.col("is_valid") == True
)

df_rejected = df_validated.filter(
    F.col("is_valid") == False
)

print("\nValid rows:", df_valid.count())
print("\nRejected rows:", df_rejected.count())

# Remove fully duplicated valid records
valid_before_dedup = df_valid.count()

df_valid = df_valid.dropDuplicates()

valid_after_dedup = df_valid.count()

duplicates_removed = valid_before_dedup - valid_after_dedup

print("\nDuplicated valid rows removed:", duplicates_removed)

# Derive simulation day from hourly step
df_valid = df_valid.withColumn(
    "transaction_day",
    F.floor((F.col("step") -1) /24) + 1
)

# Derive hour of the day in the range 0-23
df_valid = df_valid.withColumn(
    "transaction_hour",
    F.pmod(
        F.col("step") -1,
        F.lit(24)
    )
)

print("\nSilver sample with derived time fields:")

df_valid.select(
    "step",
    "transaction_day",
    "transaction_hour",
    "type",
    "amount"
).show(10, truncate=False)

# Remove internal validation columns from valid Silver records
df_valid = df_valid.drop(
    "is_valid",
    "rejection_reason"
)

print("\nFinal Silver schema:")
df_valid.printSchema()

#Persist valid Silver records
(
    df_valid.write
    .mode("overwrite")
    .parquet(str(SILVER_PATH))
)

# Persist rejected records for traceability
(
    df_rejected.write
    .mode("overwrite")
    .parquet(str(REJECTED))
)

spark.stop()