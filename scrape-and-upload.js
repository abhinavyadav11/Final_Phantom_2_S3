require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const AWS = require('aws-sdk');

// Parse ALL_CREDENTIALS env var once
const {
  apiKey,
  agentId,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  AWS_REGION,
  S3_BUCKET_NAME
} = JSON.parse(process.env.ALL_CREDENTIALS);

// Configure AWS SDK
AWS.config.update({
  accessKeyId: AWS_ACCESS_KEY_ID,
  secretAccessKey: AWS_SECRET_ACCESS_KEY,
  region: AWS_REGION
});
const s3 = new AWS.S3();

// Constants
const MAX_FETCH_RETRIES = 30;
const FETCH_RETRY_DELAY = 20000;
const MAX_LAUNCH_RETRIES = 5;
const LAUNCH_RETRY_DELAY = 10000;

// Fetch PhantomBuster output (waits with retries)
async function fetchOutput(containerId, retries = MAX_FETCH_RETRIES, delay = FETCH_RETRY_DELAY) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await axios.get(
        `https://api.phantombuster.com/api/v2/containers/fetch-output?id=${containerId}`,
        { headers: { 'X-Phantombuster-Key-1': apiKey } }
      );

      const output = res.data.output;

      if (output !== null) {
        return res.data;
      }

      console.log(`⏳ Output empty, retrying in ${delay / 1000}s... (${i + 1}/${retries})`);
      await new Promise(r => setTimeout(r, delay));
    } catch (err) {
      console.error('⚠️ Error fetching output:', err.message || err);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw new Error('❌ Output not ready after max retries');
}

// Launch PhantomBuster agent
async function launchAgentWithRetry(retries = MAX_LAUNCH_RETRIES, delay = LAUNCH_RETRY_DELAY) {
  for (let i = 0; i < retries; i++) {
    try {
      const launchRes = await axios.post(
        'https://api.phantombuster.com/api/v2/agents/launch',
        { id: agentId },
        { headers: { 'X-Phantombuster-Key-1': apiKey } }
      );
      return launchRes.data.containerId;
    } catch (err) {
      if (err.response && err.response.status === 429) {
        console.warn(`⚠️ Rate limit hit. Retrying in ${delay / 1000}s... (${i + 1}/${retries})`);
        await new Promise(res => setTimeout(res, delay));
        delay *= 2;
      } else {
        throw new Error(`❌ Failed to launch agent: ${err.message}`);
      }
    }
  }
  throw new Error('❌ Failed to launch agent after max retries');
}

// Upload a file to S3
async function uploadToS3(filePath, bucketName, key) {
  const fileContent = fs.readFileSync(filePath);

  const params = {
    Bucket: bucketName,
    Key: key,
    Body: fileContent,
    ContentType: 'application/json'
  };

  await s3.upload(params).promise();
  console.log(`✅ Uploaded to S3 bucket: ${bucketName} as ${key}`);
}

// Extract JSON URL from logs
function extractJsonUrlFromLogs(logText) {
  const jsonUrlRegex = /https:\/\/[^\s]+\.json/g;
  const matches = logText.match(jsonUrlRegex);
  return matches ? matches[0] : null;
}

// Extract CSV URL from logs
function extractCsvUrlFromLogs(logText) {
  const csvUrlRegex = /https:\/\/[^\s]+\.csv/g;
  const matches = logText.match(csvUrlRegex);
  return matches ? matches[0] : null;
}

// Main function
async function run() {
  try {
    console.log(`🚀 Launching PhantomBuster agent with ID: ${agentId}`);

    const containerId = await launchAgentWithRetry();
    console.log(`🟢 Launched agent, container ID: ${containerId}`);

    const resultRes = await fetchOutput(containerId);
    const output = resultRes.output;

    // Print full output
    console.log("📄 Full Phantom output:\n", output);

    // Extract CSV and JSON URLs from output logs
    const csvUrl = extractCsvUrlFromLogs(output);
    const jsonUrl = extractJsonUrlFromLogs(output);

    if (!csvUrl || !jsonUrl) {
      console.error("❌ Phantom output missing CSV or JSON URLs.");
      process.exit(1);
    }

    console.log("✅ Phantom CSV URL:", csvUrl);
    console.log("✅ Phantom JSON URL:", jsonUrl);

    // Save full Phantom output locally
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `phantom_output_${timestamp}.json`;
    fs.writeFileSync(fileName, JSON.stringify(resultRes, null, 2));
    console.log(`💾 Output saved locally as ${fileName}`);

    // Upload full output file to S3
    await uploadToS3(fileName, S3_BUCKET_NAME, `phantom_outputs/${fileName}`);

    // Output the URLs as a JSON object to stdout for downstream steps
    console.log(JSON.stringify({ csvUrl, jsonUrl }));

  } catch (err) {
    console.error("❌ Error:", err.message || err);
    process.exit(1);
  }
}

run();
