from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from pyspark.sql import functions as F
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SalesDataETL") \
    .config("spark.jars", "/path/to/jars/postgresql-42.x.x.jar") \
    .getOrCreate()

# Database connection properties
DB_URL = "jdbc:postgresql://<localhost>:5432/<database name>"
DB_PROPERTIES = {
    "user": "<username>",
    "password": "<password>",
    "driver": "org.postgresql.Driver"
}

DATA_DIR = "<path/to/data>"

# Function to get the latest modified file in a subdirectory
def get_latest_file(folder, subfolder, prefix):
    folder_path = os.path.join(folder, subfolder)
    if not os.path.exists(folder_path):
        print(f"Directory {folder_path} does not exist.")
        return None
    
    # Get today's date in the format that matches your file name (YYYYMMDD)
    today_date = datetime.now()
    today_date_str = today_date.strftime("%Y%m%d")
    # Get all files starting with the prefix and having today's date as suffix
    files = [f for f in os.listdir(folder_path) if f.startswith(prefix) and today_date_str in f]
    
    if not files:
        print(f"No files found for today's date in {folder_path}.")
        return None
    
    # Get the most recent file based on modification time
    latest_file = max(files, key=lambda x: os.path.getmtime(os.path.join(folder_path, x)))
    return os.path.join(folder_path, latest_file)

# Function to clean and transform data
def clean_data(df):
    return df.dropna().dropDuplicates()

# Function to process a dataset with enforced schema handling
def process_data(subfolder, file_prefix, table_name, transformations=[]):
    file_path = get_latest_file(DATA_DIR, subfolder, file_prefix)
    if not file_path:
        print(f"No new data found for {table_name}. Skipping...")
        return

    try:
        df = spark.read.option("header", True).csv(file_path)

        if df.isEmpty():
            print(f"Skipping {table_name}: No data in file {file_path}")
            return

        # Clean data (remove nulls and duplicates)
        df = clean_data(df)

        # Drop the 'id' column from the DataFrame (as primary key is auto-generated in DB)
        df = df.drop(df.columns[0])

        # Apply transformations if any
        for transform in transformations:
            df = transform(df)

        # Load the schema from the database to ensure compatibility
        db_schema = spark.read \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", f"public.{table_name}") \
            .option("user", DB_PROPERTIES["user"]) \
            .option("password", DB_PROPERTIES["password"]) \
            .option("driver", DB_PROPERTIES["driver"]) \
            .load().schema

        # Ensure the schema matches the database schema
        df = df.select(*[col(c.name).cast(c.dataType) for c in db_schema if c.name in df.columns])

        # Append data to PostgreSQL (auto-increment will handle the id generation)
        df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", f"public.{table_name}") \
            .option("user", DB_PROPERTIES["user"]) \
            .option("password", DB_PROPERTIES["password"]) \
            .option("driver", DB_PROPERTIES["driver"]) \
            .mode("append") \
            .save()

        print(f"Successfully loaded {table_name}.")

    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")

# Transformations
def transform_sales(df):
    # Calculate total price without modifying sale_id (it will be auto-generated)
    df = df.withColumn(
        "total_price", 
        col("quantity").cast("double") * col("price").cast("double")
    )
    return df

# Run ETL process
def run_etl():
    print("Starting PySpark ETL Process...")

    # Process each dataset, passing transformations as needed
    process_data("sales", "sales_", "sales", [transform_sales])

    print("ETL Process Completed.")

if __name__ == "__main__":
    run_etl()
