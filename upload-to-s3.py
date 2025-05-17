import boto3
import requests
import os
import json

# Parse JSON secret from GitHub Actions
secrets = json.loads(os.getenv("SECRET"))

AWS_ACCESS_KEY_ID = secrets["accessKeyId"]
AWS_SECRET_ACCESS_KEY = secrets["secretAccessKey"]
AWS_REGION = secrets["region"]
S3_BUCKET_NAME = secrets["bucket"]

# PhantomBuster output URLs
REMOTE_CSV_URL = 'https://phantombuster.s3.amazonaws.com/eAwTgnQzO48/bIH9f0xJSr9bmjikMzxfFA/result.csv'
REMOTE_JSON_URL = 'https://phantombuster.s3.amazonaws.com/eAwTgnQzO48/bIH9f0xJSr9bmjikMzxfFA/result.json'

def upload_file_from_url_to_s3(remote_url, s3_bucket, s3_key):
    try:
        response = requests.get(remote_url, stream=True)
        response.raise_for_status()

        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_client.upload_fileobj(response.raw, s3_bucket, s3_key)
        print(f"✅ Uploaded {remote_url} to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"❌ Error uploading {remote_url} to S3: {e}")

upload_file_from_url_to_s3(
    remote_url=REMOTE_CSV_URL,
    s3_bucket=S3_BUCKET_NAME,
    s3_key='phantombuster/data/result.csv'
)

upload_file_from_url_to_s3(
    remote_url=REMOTE_JSON_URL,
    s3_bucket=S3_BUCKET_NAME,
    s3_key='phantombuster/data/result.json'
)
