import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from logger import logger

load_dotenv()


def upload_data():
    """Upload CSV data to RustFS bucket"""

    # Initialize S3 client for RustFS
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("RUSTFS_ENDPOINT"),
        aws_access_key_id=os.getenv("RUSTFS_ROOT_USER"),
        aws_secret_access_key=os.getenv("RUSTFS_ROOT_PASSWORD"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    bucket = os.getenv("RUSTFS_BUCKET")

    # Create bucket
    try:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Created bucket: {bucket}")
    except:
        logger.info(f"Bucket {bucket} already exists")

    # Upload file
    s3.upload_file("sample_data.csv", bucket, "employee_data.csv")
    logger.info("Uploaded employee_data.csv")

    # List files
    objects = s3.list_objects_v2(Bucket=bucket)
    file_count = len(objects.get("Contents", []))
    logger.info(f"Bucket contains {file_count} file(s)")
    for obj in objects.get("Contents", []):
        logger.info(f"  - {obj['Key']}")


if __name__ == "__main__":
    upload_data()
