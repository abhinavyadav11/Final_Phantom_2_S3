import requests
import boto3
import datetime
import os
from credentials import ALL_CREDENTIALS

# 1. Prepare filename with current timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"phantom_result_{timestamp}.csv"

# 2. Launch PhantomBuster agent
phantom_launch_url = f"https://api.phantombuster.com/api/v2/agents/launch"
headers = {
    "X-Phantombuster-Key-1": ALL_CREDENTIALS["apiKey"],
    "Content-Type": "application/json"
}
launch_response = requests.post(phantom_launch_url, json={"id": ALL_CREDENTIALS["agentId"]}, headers=headers)
launch_response.raise_for_status()
print("🚀 Launched PhantomBuster agent.")

# 3. Wait for agent to complete and get output object
# Note: You can add retry logic here if needed
agent_status_url = f"https://api.phantombuster.com/api/v2/agents/fetch-output?id={ALL_CREDENTIALS['agentId']}"
output_response = requests.get(agent_status_url, headers=headers)
output_response.raise_for_status()
output_data = output_response.json()

# 4. Get download URL and fetch CSV
download_url = output_data.get("data", {}).get("container", {}).get("csvUrl")
if not download_url:
    raise Exception("❌ Failed to get download URL for CSV.")

csv_response = requests.get(download_url)
csv_response.raise_for_status()

with open(csv_filename, "wb") as f:
    f.write(csv_response.content)

print(f"📥 Downloaded CSV file: {csv_filename}")

# 5. Upload to AWS S3
s3 = boto3.client(
    's3',
    aws_access_key_id=ALL_CREDENTIALS['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=ALL_CREDENTIALS['AWS_SECRET_ACCESS_KEY'],
    region_name=ALL_CREDENTIALS['AWS_REGION']
)

bucket_name = ALL_CREDENTIALS["S3_BUCKET_NAME"]
s3_key = csv_filename

s3.upload_file(csv_filename, bucket_name, s3_key)
print(f"✅ Uploaded to S3: s3://{bucket_name}/{s3_key}")

# 6. Cleanup (optional)
os.remove(csv_filename)
