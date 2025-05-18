const axios = require('axios');
const fs = require('fs');
const AWS = require('aws-sdk');

// Parse secret JSON from environment variable
const {
  apiKey,
  agentId,
  sessionCookie, // not used unless needed for auth
  accessKeyId,
  secretAccessKey,
  bucketName,
  region
} = JSON.parse(process.env.ALL_CREDENTIALS || '{}');

// Configure AWS SDK
AWS.config.update({
  accessKeyId,
  secretAccessKey,
  region
});
const s3 = new AWS.S3();

// Constants
const MAX_FETCH_RETRIES = 30;
const FETCH_RETRY_DELAY = 20000;
const MAX_LAUNCH_RETRIES = 5;
const LAUNCH_RETRY_DELAY = 10000;

async function fetchOutput(containerId, retries = MAX_FETCH_RETRIES, delay = FETCH_RETRY_DELAY) {
  for (let i = 0; i < retries; i++) {
    const res = await axios.get(
      `https://api.phantombuster.com/api/v2/containers/fetch-output?id=${containerId}`,
      { headers: { 'X-Phantombuster-Key-1': apiKey } }
    );

    if (res.data.output !== null) {
      return res.data;
    }

    console.log(`⏳ Output empty, retrying in ${delay / 1000}s... (${i + 1}/${retries})`);
    await new Promise(r => setTimeout(r, delay));
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

async function uploadToS3(filePath, bucket, key) {
  const fileContent = fs.readFileSync(filePath);

  const params = {
    Bucket: bucket,
    Key: key,
    Body: fileContent,
    ContentType: 'application/json'
  };

  await s3.upload(params).promise();
  console.log(`✅ Uploaded to S3 bucket: ${bucket} as ${key}`);
}

async function run() {
  try {
    console.log(`🚀 Launching PhantomBuster agent with ID: ${agentId}`);

    const containerId = await launchAgentWithRetry();
    console.log(`🟢 Launched agent, container ID: ${containerId}`);

    const resultRes = await fetchOutput(containerId);
    const output = JSON.stringify(resultRes, null, 2);

    console.log("✅ Phantom output received:");

    const fileName = `phantom_output_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
    fs.writeFileSync(fileName, output);
    console.log(`💾 Output saved locally as ${fileName}`);

    await uploadToS3(fileName, bucketName, `phantom_outputs/${fileName}`);

  } catch (err) {
    console.error("❌ Error:", err.message || err);
    process.exit(1);
  }
}

run();
