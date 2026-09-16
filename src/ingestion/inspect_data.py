import pandas as pd
from pathlib import Path

DATA_PATH = Path("data/raw/PS_20174392719_1491204439457_log.csv")

df = pd.read_csv(DATA_PATH)

print("Shape:", df.shape)
print("\nColumns:")
print(df.columns.tolist())

print("\nFrist rows:")
print(df.head())

print("\nData types:")
print(df.dtypes)

print("\nNull values:")
print(df.isnull().sum())

print("\nTransactions types:")
print(df['type'].value_counts())

print("\nFraud distribution:")
print(df["isFraud"].value_counts())
