import os
import json
from datetime import datetime
import requests
import boto3
from dotenv import load_dotenv
from logger import logger

load_dotenv()


def fetch_and_upload_pokemon_data():
    """
    Fetch Pokémon data from PokéAPI and upload to S3
    Simple function that performs the work - orchestration happens in main.py
    """
    # Get S3 configuration
    bucket_name = os.getenv('S3_BUCKET_NAME')
    folder_prefix = os.getenv('S3_FOLDER_PREFIX', 'Raines')

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable not set")

    # Fetch data from API
    api_url = 'https://pokeapi.co/api/v2/pokemon?limit=151'
    logger.info(f"Fetching from {api_url}")

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    data = response.json()

    logger.info(f"Fetched {len(data.get('results', []))} Pokémon records")

    # Initialize S3 client and create key
    s3_client = boto3.client('s3')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{folder_prefix}/{timestamp}_pokemon.json"

    # Upload to S3
    logger.info(f"Uploading to s3://{bucket_name}/{s3_key}")

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )

    logger.info(f"Uploaded {len(data.get('results', []))} records")
