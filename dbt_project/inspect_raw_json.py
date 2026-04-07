import os
import json
import boto3

bucket = os.getenv("AWS_S3_BUCKET_NAME")
region = os.getenv("AWS_REGION")
key = "raw/thesportsdb/epl_season_2023-2024.json"

s3 = boto3.client("s3", region_name=region)
obj = s3.get_object(Bucket=bucket, Key=key)
data = json.loads(obj["Body"].read().decode("utf-8"))

print("Top-level Python type:", type(data).__name__)

if isinstance(data, dict):
    print("Top-level keys:", list(data.keys()))
    if "events" in data and isinstance(data["events"], list):
        print("Number of events:", len(data["events"]))
        if data["events"]:
            first = data["events"][0]
            print("First event type:", type(first).__name__)
            if isinstance(first, dict):
                print("First event keys:", list(first.keys()))
                print("First event preview:")
                print(json.dumps(first, indent=2)[:2000])
elif isinstance(data, list):
    print("Top-level list length:", len(data))
    if data:
        first = data[0]
        print("First item type:", type(first).__name__)
        if isinstance(first, dict):
            print("First item keys:", list(first.keys()))
            print("First item preview:")
            print(json.dumps(first, indent=2)[:2000])
