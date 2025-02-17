# Change Data Capture (CDC) with Airflow, PySpark, and PostgreSQL

## 📌 Project Overview

This project implements a Change Data Capture (CDC) pipeline using Apache Airflow, PySpark, and PostgreSQL to track and process sales transaction changes efficiently.

### 🚀 Features

- Daily Batch Processing (10 PM):

- Detects and processes the latest sales transaction file (sales_YYYYMMDD.csv).

- Appends new transactions to the database.

- Hourly Incremental Processing:

- Detects modifications (inserts, updates, and deletes) from the last hour.

- Applies the changes to maintain data consistency.

### 📁 Data Flow

- Sales transactions are stored as daily CSV files (sales_YYYYMMDD.csv).

- Airflow DAGs trigger the PySpark ETL process at scheduled times.

#### PySpark ETL:

- Reads the sales data.

- Cleans and transforms the data.

- Compares with existing data in PostgreSQL.

- Inserts new records, updates modified records, and deletes removed records.

### 🛠️ Technologies Used

- Apache Airflow - Workflow scheduling & orchestration.

- Apache Spark (PySpark) - Distributed data processing.

- PostgreSQL - Database for storing processed sales data.

- Python - ETL script development.

- Bash & Linux - Environment setup and automation.

### 📂 Project Structure
```
firstDataPipeline/  
│── dags/  
│   ├── etl_spark_dag.py        # Airflow DAG for scheduling ETL jobs  
│── data/  
│   ├── sales_YYYYMMDD.csv      # Daily sales transaction files  
│── etl_spark.py                # PySpark ETL script  
│── README.md                   # Project documentation  
```
### ⚡ How It Works

#### 1️⃣ Batch Processing (Daily at 10 PM)

- Runs etl_spark.py to load new sales transactions.

- Appends records to the PostgreSQL database.

#### 2️⃣ Incremental Processing (Every Hour)

- Detects modifications in the last hour.

- Updates or deletes records accordingly.

### 🚀 Setup & Installation

#### 1️⃣ Install Dependencies

Ensure the following are installed:
```
sudo apt update && sudo apt install postgresql postgresql-contrib -y
pip install pyspark psycopg2 airflow
```
#### 2️⃣ Configure Airflow

Set up Airflow and initialize the database:
```
export AIRFLOW_HOME=~/airflow
airflow db init
```
#### 3️⃣ Start Airflow Services
```
airflow webserver -p 8080 &
airflow scheduler &
```
#### 4️⃣ Add Airflow DAGs

Place etl_spark_dag.py inside dags/ and restart Airflow:
```
airflow dags list
```
### 📊 Example DAG Tasks

#### Run ETL for the latest file (daily at 10 PM):
```
airflow tasks trigger Sales_Transactions_DAG_for_pyspark run_pyspark_etl_daily
````
#### Run incremental processing (every hour):
```
airflow tasks trigger Sales_Transactions_DAG_for_pyspark run_pyspark_etl_hourly
```
### 🎯 Future Improvements

- Implement real-time CDC using Kafka.

- Optimize PostgreSQL queries for large datasets.

- Add logging and monitoring for ETL jobs.

### 📌 Author

Aspiring Data Engineer | Building projects to showcase my skills in Big Data & ETL Processing.

### Credit
Design ETL by owner / Clean and Compile by Chat-gpt
