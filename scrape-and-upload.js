const fs = require("fs");
const https = require("https");
const AWS = require("aws-sdk");

const credentials = JSON.parse(process.env.ALL_CREDENTIALS || "{}");

if (
  !credentials.apiKey ||
  !credentials.agentId ||
  !credentials.sessionCookie ||
  !credentials.accessKeyId ||
  !credentials.secretAccessKey ||
  !credentials.bucketName ||
  !credentials.region
) {
  console.error("❌ Missing required fields in ALL_CREDENTIALS");
  process.exit(1);
}

const { apiKey, agentId, sessionCookie, accessKeyId, secretAccessKey, bucketName, region } = credentials;

// Set AWS config
AWS.config.update({
  accessKeyId,
  secretAccessKey,
  region
});

const s3 = new AWS.S3();

const startAgent = () => {
  console.log(`🚀 Launching PhantomBuster agent with ID: ${agentId}`);

  const options = {
    hostname: "api.phantombuster.com",
    path: `/api/v2/agents/launch`,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Phantombuster-Key-1": apiKey,
      "Cookie": `session=${sessionCookie}`
    }
  };

  const postData = JSON.stringify({ id: agentId });

  return new Promise((resolve, reject) => {
    const req = https.request(options, res => {
      let body = "";
      res.on("data", chunk => (body += chunk));
      res.on("end", () => {
        try {
          const response = JSON.parse(body);
          const containerId = response?.containerId;
          if (containerId) {
            console.log(`🟢 Launched agent, container ID: ${containerId}`);
            resolve(containerId);
          } else {
            reject("❌ Failed to launch agent.");
          }
        } catch (err) {
          reject(`❌ Failed to parse response: ${err}`);
        }
      });
    });

    req.on("error", reject);
    req.write(postData);
    req.end();
  });
};

const getOutput = containerId => {
  const options = {
    hostname: "api.phantombuster.com",
    path: `/api/v2/containers/fetch-output?id=${containerId}`,
    method: "GET",
    headers: {
      "X-Phantombuster-Key-1": apiKey,
      "Cookie": `session=${sessionCookie}`
    }
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, res => {
      let body = "";
      res.on("data", chunk => (body += chunk));
      res.on("end", () => {
        try {
          const response = JSON.parse(body);
          if (response?.status === "success" && response.output) {
            resolve(response.output);
          } else {
            resolve(null); // still pending
          }
        } catch (err) {
          reject(`❌ Error parsing output: ${err}`);
        }
      });
    });

    req.on("error", reject);
    req.end();
  });
};

const uploadToS3 = (filePath, s3Key) => {
  const fileContent = fs.readFileSync(filePath);
  const params = {
    Bucket: bucketName,
    Key: s3Key,
    Body: fileContent
  };

  return s3.upload(params).promise();
};

(async () => {
  try {
    const containerId = await startAgent();

    let output = null;
    let retries = 30;

    for (let i = 0; i < retries; i++) {
      console.log(`⏳ Output empty, retrying in 20s... (${i + 1}/${retries})`);
      await new Promise(resolve => setTimeout(resolve, 20000));
      output = await getOutput(containerId);
      if (output) break;
    }

    if (!output) {
      throw new Error("❌ Timed out waiting for PhantomBuster output.");
    }

    const filename = `phantom_output_${new Date().toISOString().replace(/:/g, "-")}.json`;
    fs.writeFileSync(filename, JSON.stringify(output, null, 2));
    console.log(`💾 Output saved locally as ${filename}`);

    const result = await uploadToS3(filename, filename);
    console.log(`✅ Uploaded to S3: ${result.Location}`);
  } catch (error) {
    console.error("❌ Error:", error);
    process.exit(1);
  }
})();
