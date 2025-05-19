import requests
import boto3
import datetime
import os
import json

# 1. Load credentials from environment
try:
    ALL_CREDENTIALS = json.loads(os.getenv("ALL_CREDENTIALS", "{}"))
except json.JSONDecodeError:
    print("❌ Failed to parse ALL_CREDENTIALS environment variable.")
    exit(1)

required_keys = ["apiKey", "agentId", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME"]
if not all(k in ALL_CREDENTIALS for k in required_keys):
    print("❌ Missing required credentials in ALL_CREDENTIALS.")
    exit(1)

# 2. Prepare filename with current timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"phantom_result_{timestamp}.csv"

# 3. Launch PhantomBuster agent
phantom_launch_url = "https://api.phantombuster.com/api/v2/agents/launch"
headers = {
    "X-Phantombuster-Key-1": ALL_CREDENTIALS["apiKey"],
    "Content-Type": "application/json"
}
launch_response = requests.post(phantom_launch_url, json={"id": ALL_CREDENTIALS["agentId"]}, headers=headers)
launch_response.raise_for_status()
print("🚀 Launched PhantomBuster agent.")

# 4. Wait for output (you can improve this with retries + sleep)
agent_status_url = f"https://api.phantombuster.com/api/v2/agents/fetch-output?id={ALL_CREDENTIALS['agentId']}"
output_response = requests.get(agent_status_url, headers=headers)
output_response.raise_for_status()
output_data = output_response.json()

# 5. Get download URL and fetch CSV
download_url = output_data.get("data", {}).get("container", {}).get("csvUrl")
if not download_url:
    raise Exception("❌ Failed to get download URL for CSV.")

csv_response = requests.get(download_url)
csv_response.raise_for_status()

with open(csv_filename, "wb") as f:
    f.write(csv_response.content)

print(f"📥 Downloaded CSV file: {csv_filename}")

# 6. Upload to AWS S3
s3 = boto3.client(
    's3',
    aws_access_key_id=ALL_CREDENTIALS['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=ALL_CREDENTIALS['AWS_SECRET_ACCESS_KEY'],
    region_name=ALL_CREDENTIALS['AWS_REGION']
)

bucket_name = ALL_CREDENTIALS["S3_BUCKET_NAME"]
s3_key = f"phantom_outputs/{csv_filename}"

s3.upload_file(csv_filename, bucket_name, s3_key)
print(f"✅ Uploaded to S3: s3://{bucket_name}/{s3_key}")

# 7. Cleanup
os.remove(csv_filename)
print(f"🧹 Removed local file: {csv_filename}")
