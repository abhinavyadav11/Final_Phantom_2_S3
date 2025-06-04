import boto3
import requests
import os
import datetime
import pandas
from io import BytesIO

# Fetch AWS environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Print for debug
print("Access Key:", AWS_ACCESS_KEY_ID)
print("Region:", AWS_REGION)
print("Bucket Name:", S3_BUCKET_NAME)

# Fetch CSV URL from environment
CSV_URL = os.getenv("CSV_URL")

# Generate timestamp for versioning
timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
csv_s3_key_versioned = f'phantombuster/LinkedIn/result_{timestamp}.csv'
csv_s3_key_latest = 'phantombuster/LinkedIn/latest.csv'

def upload_csv_with_source_to_s3(remote_url, s3_bucket, s3_key, source_name):
    try:
        print(f"📥 Downloading CSV from: {remote_url}")
        response = requests.get(remote_url)
        response.raise_for_status()

        # Read into pandas and add source column
        df = pandas.read_csv(BytesIO(response.content))
        df['source'] = source_name  # Add the source column

        # Convert back to CSV
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Upload to S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_client.upload_fileobj(csv_buffer, s3_bucket, s3_key)
        print(f"✅ CSV uploaded with source='{source_name}' to s3://{s3_bucket}/{s3_key}")

    except Exception as e:
        print(f"❌ Error uploading CSV to S3: {e}")

if __name__ == "__main__":
    if not CSV_URL:
        print("❌ Missing CSV_URL environment variable")
        exit(1)

    source_name = os.getenv("DATA_SOURCE", "LinkedIn").lower()  # Dynamically inject source name

    # Upload versioned CSV with source column
    upload_csv_with_source_to_s3(CSV_URL, S3_BUCKET_NAME, csv_s3_key_versioned, source_name)
    
    # Upload latest CSV with source column
    upload_csv_with_source_to_s3(CSV_URL, S3_BUCKET_NAME, csv_s3_key_latest, source_name)
