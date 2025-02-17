from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from datetime import datetime, timedelta
import psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SalesDataETL") \
    .config("spark.jars", "/path/to/jars/postgresql-42.x.x.jar") \    # Set path to .jar
    .getOrCreate()

# Database connection properties
DB_URL = "jdbc:postgresql://localhost:5432/<database>"    # Insert database name
DB_PROPERTIES = {
    "user": "<username>",    # Insert postgres username
    "password": "<password>",    # Insert postgres password
    "driver": "org.postgresql.Driver"
}

DATA_DIR = "<directory/path>"    # Locate your .csv path

# Function to get the file based on the condition: today or modified in the last 1 hour
def get_file(folder, subfolder, prefix, check_modified):
    folder_path = os.path.join(folder, subfolder)
    if not os.path.exists(folder_path):
        print(f"Directory {folder_path} does not exist.")
        return None
    
    # Get today's date in the format that matches your file name (YYYYMMDD)
    today_date = datetime.now() + timedelta(days=0)
    today_date_str = today_date.strftime("%Y%m%d")
    
    # If check_modified is False, get the latest file with today's date as the suffix
    if not check_modified:
        files = [f for f in os.listdir(folder_path) if f.startswith(prefix) and today_date_str in f]
        latest_file = os.path.join(folder_path, files[0])
        print(latest_file)
        if not latest_file:
            print(f"No file found for today's date: {folder_path}. Skipping...")
            return None
        return latest_file

    # If check_modified is True, get all files modified within the last 1 hour
    else:
        files = [f for f in os.listdir(folder_path) if f.startswith(prefix)]
        modified_files = []
        for file in files:
            file_path = os.path.join(folder_path, file)
            file_mod_time = os.path.getmtime(file_path)
            if file_mod_time >= (today_date - timedelta(hours=1)).timestamp():
                modified_files.append(file_path)
        print(f"Files modified in the last 1 hour: {sorted(modified_files)}")
        if not modified_files:
            print(f"No files were modified in the last 1 hour. Skipping...")
            return None
        return sorted(modified_files)

# Function to clean and transform data
def clean_data(df):
    return df.dropna().dropDuplicates()

def process_data(subfolder, file_prefix, table_name, check_modified, transformations=[]):
    # Get the file path using the combined logic (get_file already checks for modifications)
    file_paths = get_file(DATA_DIR, subfolder, file_prefix, check_modified)
    if not file_paths:
        print(f"No data found for {table_name}. Skipping...")
        return
    print(f"Processing files: {sorted(file_paths)}")
    # Read incoming CSV data
    df = spark.read.option("header", True).csv(file_paths)

    if df.isEmpty():
        print(f"Skipping {table_name}: No data in file {file_paths}")
        return

    # Clean data (remove nulls and duplicates)
    df = clean_data(df)

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
    
    try:
        # Check if we are processing modified files
        if check_modified:
            # Loop through each file path if multiple modified files are found
            for file_path in file_paths:
                print(f"Processing file: {file_path}")

                # Establishing a psycopg2 connection (not Spark)
                conn = psycopg2.connect(
                host="127.0.0.1",
                database="firstproject",
                user=DB_PROPERTIES["user"],
                password=DB_PROPERTIES["password"]
                )
                cursor = conn.cursor()
                # Get the existing data from the database for comparison
                existing_data = spark.read \
                    .format("jdbc") \
                    .option("url", DB_URL) \
                    .option("dbtable", f"public.{table_name}") \
                    .option("user", DB_PROPERTIES["user"]) \
                    .option("password", DB_PROPERTIES["password"]) \
                    .option("driver", DB_PROPERTIES["driver"]) \
                    .load()

                # Find the rows that need to be inserted, updated, or deleted
                # Insert: New rows in the file that do not exist in the database
                inserts = df.join(existing_data, on="sale_id", how="left_anti")
                
                # Update: Rows in both the file and the database with changed values
                updates = df.join(existing_data, on="sale_id", how="inner").filter(
                    (df["customer_id"] != existing_data["customer_id"]) |
                    (df["product_id"] != existing_data["product_id"]) |
                    (df["quantity"] != existing_data["quantity"]) |
                    (df["total_price"] != existing_data["total_price"]) |
                    (df["payment_method"] != existing_data["payment_method"])
                )

                # Delete: Rows that exist in the database but are no longer in the file
                deletes = existing_data.join(df, on="sale_id", how="left_anti")
                

                # Handle deletes
                if not deletes.rdd.isEmpty():
                    delete_ids = [row.sale_id for row in deletes.collect()]
                    if delete_ids:
                        delete_query = f"""
                        DELETE FROM public.{table_name}
                        WHERE sale_id IN ({', '.join(map(str, delete_ids))})
                        """
                        cursor.execute(delete_query)

                # Handle updates
                if not updates.rdd.isEmpty():
                    for row in updates.collect():
                        update_query = f"""
                        UPDATE public.{table_name}
                        SET 
                            customer_id = %s,
                            product_id = %s,
                            quantity = %s,
                            total_price = %s,
                            payment_method = %s
                        WHERE sale_id = %s
                        """
                        cursor.execute(update_query, (row.customer_id, row.product_id, row.quantity, row.total_price, row.payment_method, row.sale_id))

                # Handle inserts
                if not inserts.rdd.isEmpty():
                    inserts.write \
                        .format("jdbc") \
                        .option("url", DB_URL) \
                        .option("dbtable", f"public.{table_name}") \
                        .option("user", DB_PROPERTIES["user"]) \
                        .option("password", DB_PROPERTIES["password"]) \
                        .option("driver", DB_PROPERTIES["driver"]) \
                        .mode("append") \
                        .save()
                conn.commit()
                cursor.close()
                conn.close()
        
        else:
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

# Run ETL process based on whether to process today's file or modified files
def run_etl(check_modified):
    print(f"Starting PySpark ETL Process with modified check = {check_modified}...")

    # Process each dataset, passing transformations as needed
    process_data("sales", "sales_", "sales",check_modified, [transform_sales])

    print(f"ETL Process {'for modified files' if check_modified else 'for the latest file'} completed.")

if __name__ == "__main__":
    # For the latest file (10 PM daily)
    run_etl(check_modified=False)
    
    # For modified previous files (triggered periodically, e.g., every hour)
    run_etl(check_modified=True)
