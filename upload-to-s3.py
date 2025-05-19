import os
import boto3
import requests

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

REMOTE_CSV_URL = os.getenv("REMOTE_CSV_URL")
REMOTE_JSON_URL = os.getenv("REMOTE_JSON_URL")

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
        raise

if __name__ == "__main__":
    print("Uploading CSV...")
    upload_file_from_url_to_s3(
        REMOTE_CSV_URL,
        S3_BUCKET_NAME,
        'phantombuster/data/result.csv'
    )

    print("Uploading JSON...")
    upload_file_from_url_to_s3(
        REMOTE_JSON_URL,
        S3_BUCKET_NAME,
        'phantombuster/data/result.json'
    )
