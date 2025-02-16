from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_pyspark_etl():
    """Function to trigger the PySpark ETL script."""
    subprocess.run(["spark-submit", "/path/to/etl_spark.py"], check=True)

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "Sales_Transactions_DAG_for_pyspark",
    default_args=default_args,
    description="DAG to run SalesTransactions PySpark ETL with PythonOperator",
    schedule_interval="0 22 * * *",
    catchup=False,
)

# PythonOperator to run PySpark ETL
run_etl_task = PythonOperator(
    task_id="run_pyspark_etl",
    python_callable=run_pyspark_etl,
    dag=dag,
)

run_etl_task
