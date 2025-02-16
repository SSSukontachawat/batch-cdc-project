# batch-cdc-project
Batch-based Change Data Capture (CDC) pipeline for sales data processing.  
  
Project Structure:  
batch-cdc-project/  
│  
├── etl_spark.py               # PySpark ETL script to process sales data  
├── etl_spark_dag.py           # Airflow DAG to schedule and run the ETL process  
├── README.md                  # Project documentation  
└── requirements.txt           # Python dependencies for the project  
  
## Prerequisites  
  
- Apache Spark 3.x  
- Python 3.x  
- Apache Airflow 2.x  
- PostgreSQL Database  
- PostgreSQL JDBC Driver (`postgresql-42.x.x.jar`)  
- Required Python Libraries listed in `requirements.txt`  

## Setup Instructions  

### 1. Install Dependencies  

Clone the repository and navigate to your project directory.  

```bash  
git clone https://github.com/yourusername/batch-cdc-project.git  
cd batch-cdc-project  
```  

Install the required Python libraries:  
```pip install -r requirements.txt```    
Note: These requirements are listed for use in my future project. Please refer to my project again later.  
  
### 2. Configure PostgreSQL  
Make sure your PostgreSQL instance is running and accessible. Update the following variables in etl_spark.py:  
  
```
DB_URL: Connection string for your PostgreSQL database.  
DB_PROPERTIES: Username and password for your PostgreSQL instance.  
DATA_DIR: Path to the directory containing your daily CSV data.  
```
    
### 3. Set Up PySpark Script  
In etl_spark.py, make sure the spark.jars configuration points to the correct location of the PostgreSQL JDBC driver JAR file.  
  
### 4. Configure Airflow DAG  
Ensure Airflow is installed and running. If you're using Docker, refer to the official Airflow Docker documentation. 
Update the etl_spark_dag.py with the correct path to your etl_spark.py file in the subprocess.run() command.  

```subprocess.run(["spark-submit", "/path/to/etl_spark.py"], check=True) # Edit your path to etl_spark.py ```  
  
### 5. Scheduling the ETL Task  
The DAG in etl_spark_dag.py is set to run at 10:00 PM UTC every day (schedule_interval="0 22 * * *"). You can modify the schedule as needed.  
  
### 6. Run the ETL Process  
You can manually trigger the DAG from the Airflow UI or let it run according to the schedule.  
```airflow webserver --port 8080```  
```airflow scheduler```  
Visit http://localhost:8080 to access the Airflow UI and monitor the pipeline execution.  
  
## File Descriptions  
### etl_spark.py  
This script handles the Extract, Transform, and Load (ETL) process:  
  
It reads CSV files for daily sales transactions.  
It transforms the data by calculating the total price of sales.  
It writes the cleaned data to a PostgreSQL database, ensuring the schema matches the database schema.  
  
### etl_spark_dag.py  
This Airflow DAG orchestrates the ETL process:  
  
It triggers the etl_spark.py script using a PythonOperator.  
The ETL process is scheduled to run daily at a specified time.  
Run the Project
Start Airflow and ensure the DAG is listed in the UI.
Trigger the DAG either manually or let it run on the defined schedule.

## Notes
Ensure your PostgreSQL instance is accessible and the required tables are already created.
The data source (CSV files) should be updated daily for the ETL process to run successfully.
Airflow is used to orchestrate and schedule the process, ensuring scalability and automation.
  
## License
MIT License  
  
## Credit:  
Design by owner, Compiled by Chat-gpt  
