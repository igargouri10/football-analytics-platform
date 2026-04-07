import os
import boto3

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
bucket = os.getenv("AWS_S3_BUCKET_NAME")

resp = s3.list_objects_v2(Bucket=bucket)
items = resp.get("Contents", [])
items = sorted(items, key=lambda x: x["LastModified"], reverse=True)

print(f"Bucket: {bucket}")
print("Most recent objects:")
for obj in items[:30]:
    print(f"{obj['LastModified']}  {obj['Size']:>8}  {obj['Key']}")
