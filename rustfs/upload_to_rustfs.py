import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from logger import logger

load_dotenv()


def upload_data(local_file_path, s3_key=None, create_bucket_if_needed=True):
    """Upload CSV data to RustFS bucket

    Args:
        local_file_path: Path to the local file to upload (e.g., 'sample_data.csv' or 'data/file.csv')
        s3_key: Name to use for the file in S3 (e.g., 'employee_data.csv')
                If not provided, uses the basename of local_file_path
        create_bucket_if_needed: Whether to attempt creating the bucket if it doesn't exist (default: True)

    Example usage:
        # Upload with custom S3 key name
        upload_data('sample_data.csv', 'employee_data.csv')

        # Upload using the same filename
        upload_data('customers.csv')  # Will be uploaded as 'customers.csv'

        # Upload multiple files
        files = [
            ('sample_data.csv', 'employee_data.csv'),
            ('products.csv', 'product_data.csv'),
            ('orders.csv', 'order_data.csv')
        ]
        for local_file, s3_name in files:
            upload_data(local_file, s3_name)
    """

    # Initialize S3 client for RustFS using credentials from environment variables
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("RUSTFS_ENDPOINT"),
        aws_access_key_id=os.getenv("RUSTFS_ROOT_USER"),
        aws_secret_access_key=os.getenv("RUSTFS_ROOT_PASSWORD"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    bucket = os.getenv("RUSTFS_BUCKET")

    # Create bucket if needed and if create_bucket_if_needed is True
    if create_bucket_if_needed:
        try:
            s3.create_bucket(Bucket=bucket)
            logger.info(f"Created bucket: {bucket}")
        except:
            logger.info(f"Bucket {bucket} already exists")

    # If s3_key is not provided, use the basename of the local file
    if s3_key is None:
        s3_key = os.path.basename(local_file_path)

    # Upload the file using the provided parameters
    s3.upload_file(local_file_path, bucket, s3_key)
    logger.info(f"Uploaded {s3_key} from {local_file_path}")

    # List files
    objects = s3.list_objects_v2(Bucket=bucket)
    file_count = len(objects.get("Contents", []))
    logger.info(f"Bucket contains {file_count} file(s)")
    for obj in objects.get("Contents", []):
        logger.info(f"  - {obj['Key']}")


if __name__ == "__main__":
    # Example 1: Upload with custom S3 key name
    upload_data('sample_data.csv', 'employee_data.csv')

    # Example 2: Upload multiple files in a loop
    # Uncomment the code below to upload multiple files
    # files_to_upload = [
    #     ('sample_data.csv', 'employee_data.csv'),
    #     ('customers.csv', 'customer_data.csv'),
    #     ('products.csv', 'product_data.csv')
    # ]
    # for local_file, s3_name in files_to_upload:
    #     upload_data(local_file, s3_name, create_bucket_if_needed=False)  # Only create bucket once
