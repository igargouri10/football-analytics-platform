# scripts/ingest_data.py - FIXED VERSION

import os
import requests
import boto3
import json

# --- CONFIGURATION ---
# TheSportsDB API settings (Free, no token required)
# English Premier League (id=4328), 2023-2024 Season
API_URL = "https://www.thesportsdb.com/api/v1/json/3/eventsseason.php?id=4328&s=2023-2024"

# AWS settings - uses variables from the .env file loaded by Docker/Airflow
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# Use variable names consistent with the rest of the project
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# Initialize the S3 client
S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def fetch_data_from_api():
    """Fetches match data from TheSportsDB API."""
    print("Fetching data from API...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an exception for HTTP error codes
        print("Successfully fetched data from API.")
        data = response.json()
        # The free API can sometimes return empty results, so we check
        if not data or not data.get("events"):
            print("No event data found in the API response.")
            return None
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
        return None
    except Exception as err:
        print(f"Other error occurred: {err}")
        return None

def upload_to_s3(data, bucket_name):
    """Uploads data as a single JSON file to S3, overwriting the previous version."""
    if not data:
        print("No data to upload.")
        return

    # Use a static filename because we are fetching the entire season's data at once.
    # This file will be overwritten on each run, providing a consistent source for dbt.
    file_name = "raw/thesportsdb/epl_season_2023-2024.json"

    print(f"Uploading data to S3 bucket '{bucket_name}' as '{file_name}'...")
    try:
        json_data = json.dumps(data, indent=4)
        S3_CLIENT.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        print("Successfully uploaded data to S3.")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

if __name__ == "__main__":
    print("--- Starting Ingestion Pipeline ---")
    # A simple check to ensure AWS credentials are set
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME, AWS_REGION]):
        print("Error: Missing required AWS environment variables.")
    else:
        api_data = fetch_data_from_api()
        upload_to_s3(api_data, S3_BUCKET_NAME)
    print("--- Ingestion Pipeline Finished ---")