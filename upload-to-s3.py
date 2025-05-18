import os
import sys
import json
import boto3
from botocore.exceptions import BotoCoreError, ClientError

def upload_to_s3(file_path, bucket_name, s3_key, aws_access_key_id, aws_secret_access_key, aws_region):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    try:
        s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=s3_key,
            ExtraArgs={'ContentType': 'application/json'}
        )
        print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except (BotoCoreError, ClientError) as e:
        print(f"❌ Failed to upload file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check command-line args
    if len(sys.argv) != 3:
        print("Usage: python upload-to-s3.py <local_file_path> <s3_key>")
        sys.exit(1)

    local_file = sys.argv[1]
    s3_key = sys.argv[2]

    # Read ALL_CREDENTIALS env var and parse JSON
    all_creds_json = os.getenv('ALL_CREDENTIALS')
    if not all_creds_json:
        print("❌ Environment variable ALL_CREDENTIALS is not set.")
        sys.exit(1)

    try:
        creds = json.loads(all_creds_json)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse ALL_CREDENTIALS JSON: {e}")
        sys.exit(1)

    aws_access_key_id = creds.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = creds.get('AWS_SECRET_ACCESS_KEY')
    aws_region = creds.get('AWS_REGION')
    bucket_name = creds.get('S3_BUCKET_NAME')

    if not all([aws_access_key_id, aws_secret_access_key, aws_region, bucket_name]):
        print("❌ Missing AWS credentials or bucket name in ALL_CREDENTIALS.")
        sys.exit(1)

    upload_to_s3(local_file, bucket_name, s3_key, aws_access_key_id, aws_secret_access_key, aws_region)
