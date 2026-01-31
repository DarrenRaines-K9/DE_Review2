import os
import io
import boto3
from dotenv import load_dotenv
import polars as pl
import snowflake.connector
from logger import logger

load_dotenv()


def load_s3_to_snowflake():
    """Retrieve CSV from S3, load into memory with Polars, and write to Snowflake"""

    # Get S3 configuration
    aws_profile = os.getenv('AWS_PROFILE')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    s3_prefix = os.getenv('S3_FOLDER_PREFIX', '')

    # Build S3 key
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
    df = pl.read_csv(csv_buffer, infer_schema_length=None)
    logger.info(f"Loaded {len(df):,} rows x {len(df.columns)} columns")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    cursor = conn.cursor()

    logger.info("Connected to Snowflake")

    # Clean column names for Snowflake
    df = df.rename({
        col: col.replace(' ', '_').replace('(', '').replace(')', '').replace('.', '_')
        for col in df.columns
    })

    # Map Polars types to Snowflake types
    type_map = {
        'Int64': 'INTEGER',
        'Float64': 'FLOAT',
        'Boolean': 'BOOLEAN',
        'Utf8': 'VARCHAR',
        'String': 'VARCHAR',
        'Date': 'DATE',
        'Datetime': 'TIMESTAMP'
    }

    # Generate CREATE TABLE statement
    columns = []
    for col_name, col_type in df.schema.items():
        col_type_str = str(col_type)
        sf_type = 'VARCHAR'
        for pl_type, sf in type_map.items():
            if pl_type in col_type_str:
                sf_type = sf
                break
        columns.append(f'"{col_name}" {sf_type}')

    table_name = 'NPPES_SAMPLE'

    # Create table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    cursor.execute(create_sql)
    logger.info(f"Created table '{table_name}'")

    # Insert data
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

    data = df.rows()
    cursor.executemany(insert_sql, data)
    conn.commit()

    logger.info(f"Inserted {len(df):,} rows")

    # Verify
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    logger.info(f"Verified {count:,} rows in '{table_name}'")

    cursor.close()
    conn.close()
    logger.info("Connection closed")


if __name__ == "__main__":
    load_s3_to_snowflake()
