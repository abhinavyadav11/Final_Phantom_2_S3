import boto3
import requests
import os
import datetime

# Fetch AWS environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Print for debug
print("Access Key:", AWS_ACCESS_KEY_ID)
print("Region:", AWS_REGION)
print("Bucket Name:", S3_BUCKET_NAME)

# Fetch file URLs from environment
CSV_URL = os.getenv("CSV_URL")
JSON_URL = os.getenv("JSON_URL")

# Generate timestamp for versioning
timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
csv_s3_key_versioned = f'phantombuster/data/result_{timestamp}.csv'
json_s3_key_versioned = f'phantombuster/data/result_{timestamp}.json'
csv_s3_key_latest = 'phantombuster/data/latest.csv'
json_s3_key_latest = 'phantombuster/data/latest.json'

def upload_file_from_url_to_s3(remote_url, s3_bucket, s3_key):
    try:
        print(f"📥 Downloading from URL: {remote_url}")
        response = requests.get(remote_url, stream=True)
        response.raise_for_status()

        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_client.upload_fileobj(response.raw, s3_bucket, s3_key)
        print(f"✅ Uploaded to s3://{s3_bucket}/{s3_key}")

    except Exception as e:
        print(f"❌ Error uploading {remote_url} to S3: {e}")

if __name__ == "__main__":
    if not CSV_URL or not JSON_URL:
        print("❌ Missing CSV_URL or JSON_URL environment variables")
        exit(1)

    # Upload versioned copies
    upload_file_from_url_to_s3(CSV_URL, S3_BUCKET_NAME, csv_s3_key_versioned)
    upload_file_from_url_to_s3(JSON_URL, S3_BUCKET_NAME, json_s3_key_versioned)

    # Upload latest copies
    upload_file_from_url_to_s3(CSV_URL, S3_BUCKET_NAME, csv_s3_key_latest)
    upload_file_from_url_to_s3(JSON_URL, S3_BUCKET_NAME, json_s3_key_latest)

