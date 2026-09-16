# financial-transactions-data-pipeline
End-to-end data engineering pipeline for financial transaction data using Python, PySpark, SQL and a Medallion Arqchitecture, progressively extended with Airflow and cloud services.

## Dataset Overview

The project uses the PaySim synthetic financial transactions daraset.

### Initial profiling:

- 6,362,620 transactions
- 11 columns
- 5 transaction types
- No missing values detected
- No duplicated rows
- 8,213 fraudelent transactions
- Fraud occurs only in 'TRANSFER' and 'CASH_OUT' transactions 
- Only 16 transactions are marked as 'isFlaggedFraud'
- Transaction steps range from 1 to 743