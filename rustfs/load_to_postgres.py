import os
import boto3
import polars as pl
import psycopg2
from botocore.client import Config
from dotenv import load_dotenv
from logger import logger

load_dotenv()


def load_data(s3_key, table_name, primary_key_column="id", local_file_path=None):
    """
    Download data from RustFS and load into PostgreSQL

    Parameters:
    -----------
    s3_key : str
        The file name/key in RustFS to download (e.g., 'employee_data.csv')
    table_name : str
        The name of the PostgreSQL table to create/insert into (e.g., 'employees')
    primary_key_column : str, optional
        The column to use as primary key for upserts (default: 'id')
    local_file_path : str, optional
        Where to save the downloaded file. If None, saves to /tmp/{s3_key}

    Examples:
    ---------
    # Load employee data
    load_data('employee_data.csv', 'employees')

    # Load customer data with custom primary key
    load_data('customers.csv', 'customers', primary_key_column='customer_id')

    # Specify custom local path
    load_data('products.csv', 'products', local_file_path='/tmp/my_products.csv')
    """
    # Default local path if not provided
    if local_file_path is None:
        local_file_path = f"/tmp/{s3_key}"

    # Initialize S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("RUSTFS_ENDPOINT"),
        aws_access_key_id=os.getenv("RUSTFS_ROOT_USER"),
        aws_secret_access_key=os.getenv("RUSTFS_ROOT_PASSWORD"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Download file from RustFS
    bucket = os.getenv("RUSTFS_BUCKET")
    s3.download_file(bucket, s3_key, local_file_path)
    logger.info(f"Downloaded {s3_key} from RustFS to {local_file_path}")

    # Read CSV with Polars
    df = pl.read_csv(local_file_path)
    logger.info(f"Loaded {len(df)} rows with Polars")

    # Connect to Postgres
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    # Generate table schema from Polars DataFrame
    columns = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        # Map Polars dtypes to PostgreSQL types
        if dtype == pl.Int64 or dtype == pl.Int32:
            pg_type = "INTEGER"
        elif dtype == pl.Float64 or dtype == pl.Float32:
            pg_type = "NUMERIC"
        elif dtype == pl.Date:
            pg_type = "DATE"
        elif dtype == pl.Datetime:
            pg_type = "TIMESTAMP"
        elif dtype == pl.Boolean:
            pg_type = "BOOLEAN"
        else:
            pg_type = "TEXT"

        # Mark primary key column
        if col_name == primary_key_column:
            columns.append(f"{col_name} {pg_type} PRIMARY KEY")
        else:
            columns.append(f"{col_name} {pg_type}")

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
    """
    cur.execute(create_table_sql)
    logger.info(f"Table '{table_name}' created/verified")

    # Generate dynamic INSERT statement
    column_names = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))

    # Build the ON CONFLICT clause for upsert
    update_clauses = [f"{col} = EXCLUDED.{col}" for col in df.columns if col != primary_key_column]

    insert_sql = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT ({primary_key_column}) DO UPDATE SET
            {', '.join(update_clauses)}
    """

    # Insert data
    for row in df.iter_rows():
        cur.execute(insert_sql, row)

    conn.commit()
    logger.info(f"Inserted {len(df)} rows into '{table_name}' table")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_data("employee_data.csv", "employees")
