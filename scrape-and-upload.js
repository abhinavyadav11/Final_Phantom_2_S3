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

async function fetchOutput(containerId, retries = MAX_FETCH_RETRIES, delay = FETCH_RETRY_DELAY) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await axios.get(
        `https://api.phantombuster.com/api/v2/containers/fetch-output?id=${containerId}`,
        { headers: { 'X-Phantombuster-Key-1': apiKey } }
      );

      if (res.data.output !== null && res.data.output.length > 0) {
        return res.data;
      }

      console.log(`⏳ Output empty, retrying in ${delay / 1000}s... (${i + 1}/${retries})`);
      await new Promise(r => setTimeout(r, delay));
    } catch (err) {
      console.error('Error fetching output:', err.message || err);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw new Error('❌ Output not ready after max retries');
}

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
        throw err;
      }
    }
  }
  throw new Error('❌ Failed to launch agent after max retries due to rate limiting');
}

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

function extractUrlsFromOutput(outputString) {
  // Regex to find .json and .csv URLs in output logs
  const urlRegex = /(https:\/\/phantombuster\.s3\.amazonaws\.com\/[^\s"]+?\.(json|csv))/g;

  const matches = [...outputString.matchAll(urlRegex)];
  const urls = { jsonUrl: null, csvUrl: null };

  for (const match of matches) {
    const url = match[1];
    if (url.endsWith('.json') && !urls.jsonUrl) {
      urls.jsonUrl = url;
    } else if (url.endsWith('.csv') && !urls.csvUrl) {
      urls.csvUrl = url;
    }
  }

  return urls;
}

async function run() {
  try {
    console.log(`🚀 Launching PhantomBuster agent with ID: ${agentId}`);

    const containerId = await launchAgentWithRetry();
    console.log(`🟢 Launched agent, container ID: ${containerId}`);

    const resultRes = await fetchOutput(containerId);

    // The output is a big string with logs
    const outputStr = resultRes.output;

    console.log("Full Phantom output (truncated to 500 chars):", outputStr.substring(0, 500));

    const { jsonUrl, csvUrl } = extractUrlsFromOutput(outputStr);

    if (!jsonUrl) {
      console.error("❌ Phantom output does not contain a JSON result URL.");
      process.exit(1);
    }

    console.log("✅ Phantom JSON result URL:", jsonUrl);
    if (csvUrl) console.log("✅ Phantom CSV result URL:", csvUrl);

    // Save full API response locally
    const fileName = `phantom_output_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
    fs.writeFileSync(fileName, JSON.stringify(resultRes, null, 2));
    console.log(`💾 Output saved locally as ${fileName}`);

    // Save JSON URL to separate file
    const urlFileName = 'phantom_result_url.txt';
    fs.writeFileSync(urlFileName, jsonUrl);
    console.log(`💾 JSON result URL saved locally as ${urlFileName}`);

    // Upload full output JSON to S3
    await uploadToS3(fileName, S3_BUCKET_NAME, `phantom_outputs/${fileName}`);

    // Optional: Download and upload the JSON result file from PhantomBuster S3 to your bucket
    // You can implement if needed, just ask!

  } catch (err) {
    console.error("❌ Error:", err.message || err);
    process.exit(1);
  }
}

run();
