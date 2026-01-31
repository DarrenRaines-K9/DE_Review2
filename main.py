import os
import sys
from dotenv import load_dotenv
from logger import logger
from rustfs.upload_to_rustfs import upload_data
from rustfs.load_to_postgres import load_data
from duck_db.setup import setup_duckdb
from API.api_to_s3 import fetch_and_upload_pokemon_data
from s3_duckdb.s3_to_duckdb import load_s3_to_duckdb
from Snowflake.s3_to_snowflake import load_s3_to_snowflake

load_dotenv()


def main():
    """Orchestrate the complete data pipeline"""

    logger.info("=" * 60)
    logger.info("DATA PIPELINE STARTING")
    logger.info("=" * 60)

    # Get configuration
    bucket_name = os.getenv("RUSTFS_BUCKET")
    rustfs_endpoint = os.getenv("RUSTFS_ENDPOINT")
    postgres_host = os.getenv("POSTGRES_HOST")

    logger.info(f"RustFS Endpoint: {rustfs_endpoint}")
    logger.info(f"Target Bucket: {bucket_name}")
    logger.info(f"PostgreSQL Host: {postgres_host}")

    try:
        # Step 1: Upload to RustFS
        logger.info("\n" + "=" * 60)
        logger.info("STEP 1: Upload to RustFS")
        logger.info("=" * 60)
        upload_data()

        # Step 2: Load to PostgreSQL
        logger.info("\n" + "=" * 60)
        logger.info("STEP 2: Load to PostgreSQL")
        logger.info("=" * 60)
        load_data()

        # Step 3: Setup DuckDB
        logger.info("\n" + "=" * 60)
        logger.info("STEP 3: Setup DuckDB")
        logger.info("=" * 60)
        setup_duckdb()

        # Step 4: Fetch and Upload Pokemon Data to S3
        logger.info("\n" + "=" * 60)
        logger.info("STEP 4: Fetch and Upload Pokemon Data to S3")
        logger.info("=" * 60)
        fetch_and_upload_pokemon_data()

        # Step 5: Load S3 CSV to DuckDB
        logger.info("\n" + "=" * 60)
        logger.info("STEP 5: Load S3 CSV to DuckDB")
        logger.info("=" * 60)
        load_s3_to_duckdb()

        # Step 6: Load S3 CSV to Snowflake
        logger.info("\n" + "=" * 60)
        logger.info("STEP 6: Load S3 CSV to Snowflake")
        logger.info("=" * 60)
        load_s3_to_snowflake()

        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"\nData loaded to 'employees' table (Postgres)")
        logger.info(f"Data loaded to 'products' table (DuckDB)")
        logger.info(f"Pokemon data uploaded to S3")
        logger.info(f"NPPES sample data loaded to DuckDB from S3")
        logger.info(f"NPPES sample data loaded to Snowflake from S3")
        logger.info(f"Query using SQLTools in VSCode")

    except Exception as e:
        logger.error("=" * 60)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
