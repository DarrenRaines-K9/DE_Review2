import os
import io
import boto3
from dotenv import load_dotenv
import polars as pl
import duckdb
from logger import logger

load_dotenv()


def load_s3_to_duckdb(filename, table_name, db_path=None, use_prefix=True):
    """
    Retrieve CSV from S3, load into memory with Polars, and write to DuckDB

    Parameters:
    -----------
    filename : str
        The CSV filename in S3 (e.g., 'nppes_sample.csv')
    table_name : str
        The name of the DuckDB table to create (e.g., 'nppes_sample')
    db_path : str, optional
        Path to the DuckDB database file. If None, uses 's3_duckdb/{table_name}.duckdb'
    use_prefix : bool, optional
        Whether to use the S3_FOLDER_PREFIX from env vars (default: True)

    Examples:
    ---------
    # Load NPPES data with default settings
    load_s3_to_duckdb('nppes_sample.csv', 'nppes_sample')

    # Load customer data without prefix
    load_s3_to_duckdb('customers.csv', 'customers', use_prefix=False)

    # Specify custom database path
    load_s3_to_duckdb('products.csv', 'products', db_path='data/my_products.duckdb')
    """
    # Default database path if not provided
    if db_path is None:
        db_path = f's3_duckdb/{table_name}.duckdb'

    # Get S3 configuration
    aws_profile = os.getenv('AWS_PROFILE')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    s3_prefix = os.getenv('S3_FOLDER_PREFIX', '')

    # Build S3 key with prefix if requested
    if use_prefix and s3_prefix:
        s3_key = f"{s3_prefix}/{filename}"
    else:
        s3_key = filename

    # Download from S3 to memory
    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client('s3')

    logger.info(f"Downloading s3://{s3_bucket}/{s3_key} to memory")

    csv_buffer = io.BytesIO()
    s3_client.download_fileobj(s3_bucket, s3_key, csv_buffer)
    csv_buffer.seek(0)

    logger.info(f"Downloaded {csv_buffer.getbuffer().nbytes:,} bytes")

    # Read CSV with Polars
    # Use infer_schema_length=None to scan entire file for accurate type detection
    df = pl.read_csv(csv_buffer, infer_schema_length=None)
    logger.info(f"Loaded {len(df):,} rows x {len(df.columns)} columns")
    logger.info(
        f"Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")

    # Write to DuckDB
    conn = duckdb.connect(db_path)

    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"Wrote {row_count:,} rows to DuckDB table '{table_name}'")

    conn.close()
    logger.info(f"Data saved to {db_path}")


if __name__ == "__main__":
    load_s3_to_duckdb('nppes_sample.csv', 'nppes_sample')
