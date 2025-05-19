import json
import glob
import re
import requests
import boto3
import os
from datetime import datetime

# Step 1: Find latest phantom_output_*.json
json_files = sorted(glob.glob("phantom_output_*.json"), key=os.path.getmtime)
if not json_files:
    raise FileNotFoundError("❌ No PhantomBuster output JSON file found.")
latest_json_file = json_files[-1]
print(f"📄 Latest JSON file found: {latest_json_file}")

# Step 2: Load JSON content
with open(latest_json_file, "r") as f:
    data = json.load(f)

# Step 3: Extract CSV URL using regex
output_str = data.get("output", "")
csv_match = re.search(r"https://phantombuster\.s3\.amazonaws\.com/[^\s\"']+\.csv", output_str)

if not csv_match:
    raise ValueError("❌ CSV URL not found in PhantomBuster JSON output.")
CSV_URL = csv_match.group()
print(f"✅ Extracted CSV URL: {CSV_URL}")

# Step 4: Download CSV
csv_filename = f"phantom_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
response = requests.get(CSV_URL)
if response.status_code != 200:
    raise Exception(f"❌ Failed to download CSV. Status code: {response.status_code}")
with open(csv_filename, "wb") as f:
    f.write(response.content)
print(f"📥 Downloaded CSV file: {csv_filename}")

# Step 5: Upload to S3
# Replace these with your actual S3 bucket name and path
bucket_name = "phantombusterdata"
s3_key = f"phantom_outputs/{csv_filename}"

s3 = boto3.client('s3')
s3.upload_file(csv_filename, bucket_name, s3_key)
print(f"✅ Uploaded to S3: s3://{bucket_name}/{s3_key}")
