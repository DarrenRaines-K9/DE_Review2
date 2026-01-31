import os
import io
import boto3
from dotenv import load_dotenv
import polars as pl
import duckdb
from logger import logger

load_dotenv()


def load_s3_to_duckdb():
    """Retrieve CSV from S3, load into memory with Polars, and write to DuckDB"""

    # Get S3 configuration
    aws_profile = os.getenv('AWS_PROFILE')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    s3_prefix = os.getenv('S3_FOLDER_PREFIX', '')

    # Build S3 key with prefix
    filename = 'nppes_sample.csv'
    s3_key = f"{s3_prefix}/{filename}" if s3_prefix else filename

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
    db_path = 's3_duckdb/nppes_data.duckdb'
    conn = duckdb.connect(db_path)

    conn.execute("CREATE TABLE IF NOT EXISTS nppes_sample AS SELECT * FROM df")

    row_count = conn.execute("SELECT COUNT(*) FROM nppes_sample").fetchone()[0]
    logger.info(f"Wrote {row_count:,} rows to DuckDB table 'nppes_sample'")

    conn.close()
    logger.info(f"Data saved to {db_path}")


if __name__ == "__main__":
    load_s3_to_duckdb()
