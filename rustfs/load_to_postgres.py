import os
import boto3
import polars as pl
import psycopg2
from botocore.client import Config
from dotenv import load_dotenv
from logger import logger

load_dotenv()


def load_data():
    """Download data from RustFS and load into PostgreSQL"""

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
    s3.download_file(bucket, "employee_data.csv", "/tmp/employee_data.csv")
    logger.info("Downloaded file from RustFS")

    # Read CSV with Polars
    df = pl.read_csv("/tmp/employee_data.csv")
    logger.info(f"Loaded {len(df)} rows with Polars")

    # Connect to Postgres
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    # Create table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(50),
            salary INTEGER,
            hire_date DATE
        )
    """
    )
    logger.info("Table 'employees' created/verified")

    # Insert data
    for row in df.iter_rows():
        cur.execute(
            """
            INSERT INTO employees (id, name, department, salary, hire_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                department = EXCLUDED.department,
                salary = EXCLUDED.salary,
                hire_date = EXCLUDED.hire_date
        """,
            row,
        )

    conn.commit()
    logger.info(f"Inserted {len(df)} rows into 'employees' table")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_data()
