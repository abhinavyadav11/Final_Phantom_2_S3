const fs = require("fs");
const axios = require("axios");
const AWS = require("aws-sdk");

// Load credentials from environment variable
let credentials;
try {
  credentials = JSON.parse(process.env.ALL_CREDENTIALS);
} catch (error) {
  console.error("❌ Failed to parse ALL_CREDENTIALS:", error);
  process.exit(1);
}

const {
  apiKey,
  agentId,
  sessionCookie,
  accessKeyId,
  secretAccessKey
} = credentials;

const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;
const AWS_REGION = process.env.AWS_REGION;

if (!S3_BUCKET_NAME) {
  console.error("❌ S3_BUCKET_NAME is not set.");
  process.exit(1);
}

// Configure AWS SDK
AWS.config.update({
  accessKeyId,
  secretAccessKey,
  region: AWS_REGION || "ap-south-1"
});

const s3 = new AWS.S3();

async function launchPhantomBusterAgent() {
  console.log("🚀 Launching PhantomBuster agent with ID:", agentId);

  const launchUrl = `https://api.phantombuster.com/api/v2/agents/launch`;
  const response = await axios.post(
    launchUrl,
    { id: agentId },
    {
      headers: {
        "X-Phantombuster-Key-1": apiKey,
        Cookie: `session=${sessionCookie}`
      }
    }
  );

  const containerId = response.data.containerId;
  console.log("🟢 Launched agent, container ID:", containerId);

  const outputUrl = `https://api.phantombuster.com/api/v2/containers/fetch-output?id=${containerId}`;
  let output;

  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 20000)); // wait 20s
    const res = await axios.get(outputUrl, {
      headers: {
        "X-Phantombuster-Key-1": apiKey,
        Cookie: `session=${sessionCookie}`
      }
    });

    if (res.data.output) {
      output = res.data.output;
      break;
    } else {
      console.log(`⏳ Output empty, retrying in 20s... (${i + 1}/30)`);
    }
  }

  if (!output) {
    throw new Error("❌ PhantomBuster output not available after multiple retries.");
  }

  const filename = `phantom_output_${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
  fs.writeFileSync(filename, JSON.stringify(output, null, 2));
  console.log("✅ Phantom output received:");
  console.log(`💾 Output saved locally as ${filename}`);
  return filename;
}

async function uploadToS3(filename) {
  const fileContent = fs.readFileSync(filename);

  const params = {
    Bucket: S3_BUCKET_NAME,
    Key: filename,
    Body: fileContent,
    ContentType: "application/json"
  };

  await s3.upload(params).promise();
  console.log(`✅ Uploaded ${filename} to S3 bucket: ${S3_BUCKET_NAME}`);
}

(async () => {
  try {
    const filename = await launchPhantomBusterAgent();
    await uploadToS3(filename);
  } catch (error) {
    console.error("❌ Error:", error.message || error);
    process.exit(1);
  }
})();
