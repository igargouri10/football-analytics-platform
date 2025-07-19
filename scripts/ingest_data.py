# scripts/ingest_data.py

import os
import requests
import boto3
from datetime import datetime
from dotenv import load_dotenv
import json

# --- CONFIGURATION ---
# Load environment variables from .env file
load_dotenv()

# Football API settings
API_TOKEN = os.getenv("API_FOOTBALL_TOKEN")
API_URL = "https://api.football-data.org/v4/competitions/PL/matches" # PL = Premier League
API_HEADERS = {
    "X-Auth-Token": API_TOKEN,
    "Accept": "application/json"
}

# AWS settings
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_S3_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def fetch_data_from_api():
    """Fetches match data from the football-data.org API."""
    print("Fetching data from API...")
    try:
        response = requests.get(API_URL, headers=API_HEADERS)
        # This will raise an exception for HTTP error codes (4xx or 5xx)
        response.raise_for_status() 
        print("Successfully fetched data from API.")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        # Print the full response text to see the detailed error from the API
        print(f"Response content: {response.text}")
        return None
    except Exception as err:
        print(f"Other error occurred: {err}")
        return None

def upload_to_s3(data, bucket_name):
    """Uploads data as a JSON file to an S3 bucket."""
    if not data:
        print("No data to upload.")
        return

    today_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"raw/matches/{today_str}.json"

    print(f"Uploading data to S3 bucket '{bucket_name}' as '{file_name}'...")
    try:
        # Convert data to JSON string
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
        raise e


if __name__ == "__main__":
    print("--- Starting Ingestion Pipeline ---")
    api_data = fetch_data_from_api()
    upload_to_s3(api_data, S3_BUCKET_NAME)
    print("--- Ingestion Pipeline Finished ---")