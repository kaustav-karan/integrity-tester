const express = require("express");
const multer = require("multer");
const { MinioClient } = require("./minioClient");
const Redis = require("ioredis");
const fs = require("fs");
const path = require("path");
const bodyParser = require("body-parser");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
require("dotenv").config();
const axios = require("axios")

const app = express();
app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

const PORT = 3000;

const MINIO_BUCKET = process.env.MINIO_BUCKET || "";

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const clients = new Map();

// Setup multer
const upload = multer({ dest: "uploads/" });

// Redis setup
const redis = new Redis(); // defaults to localhost:6379

// MinIO client
const minioClient = MinioClient(); // factory below

// Ensure uploads directory exists
const uploadsDir = path.join(__dirname, "uploads");
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

// Local logger
const logFile = path.join(__dirname, "logs", "uploads.log");
fs.mkdirSync(path.dirname(logFile), { recursive: true }); // Moved up
const log = (msg) =>
  fs.appendFileSync(logFile, `[${new Date().toISOString()}] ${msg}\n`);

// WebSocker handeling
wss.on("connection", (ws, req) => {
  const id = Date.now().toString();
  clients.set(id, ws);
  console.log(`Client connected: ${id}`);

  ws.send(JSON.stringify({ event: "connected", id }));

  ws.on("close", () => {
    clients.delete(id);
    console.log(`Client disconnected: ${id}`);
  });
});

function broadcast(data) {
  const message = JSON.stringify(data);
  clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(message);
      } catch (err) {
        console.error("WebSocket send error:", err);
      }
    }
  });

}

// presigned URL variable needs to be shared between multiple endpoints hence making it global
let presignedUrl = "";

// Upload route
app.post("/upload", upload.single("file"), async (req, res) => {
  const file = req.file;
  if (!file) return res.status(400).send("No file uploaded.");

  const objectName = file.originalname;
  const filePath = file.path;
  const localFileID = uuidv4();

  try {
    // Upload to MinIO
    broadcast({ event: "uploading", msg: `Uploading ${objectName}` });
    await minioClient.fPutObject(MINIO_BUCKET, objectName, filePath);
    presignedUrl = await minioClient.presignedGetObject(MINIO_BUCKET, objectName, 0);
    log(`Uploaded: ${objectName}`);
    broadcast({
      event: "uploaded",
      msg: `Uploaded ${objectName} to MinIO`,
    });
    fs.unlink(filePath, (err) => {
      if (err) console.error("Failed to delete temp file:", err);
    });


    // Push message to Redis
    await redis.lpush(
      "analysisQueue",
      JSON.stringify({
        objectName,
        bucket: MINIO_BUCKET,
      })
    );
    broadcast({
      event: "queued",
      msg: `Queued ${objectName} for analysis`,
    });
    res.status(200).json({ message: "File uploaded and queued for analysis." });
  } catch (err) {
    console.error(err);
    broadcast({
      event: "error",
      msg: `Error uploading ${objectName}: ${err.message}`,
    });
    res.status(500).send("Upload failed.");
  }
});

// Endpoint for analyzer to post results
app.post("/analysis-result", bodyParser.json(), async (req, res) => {
  const { objectName, passed, report } = req.body;
  let refID = "";
  log(`Analysis result for ${objectName}: ${passed ? "PASSED" : "FAILED"}`);
  
  if (!passed) {
    try {
      await minioClient.removeObject(MINIO_BUCKET, objectName);
      log(`Removed ${objectName} due to failed analysis.`);
    } catch (e) {
      console.error("Failed to remove file:", e);
    }
  }else {
    //if passed then first send a refID generation request to the main server
    try {
      // Post to main server and receive refID
      const response = await axios.get(`http://${process.env.MAIN_SERVER}/generateRefId`);
      const { refID } = response.data;
    } catch (err) {
      console.error("Error posting to main server:", err);
      return res.status(500).send("Failed to process analysis result.");
    }

    //once refID recieved then do a post request to the encoder
    try {
      await axios.post(
        `http://${ENCODER}:${ENCODER_PORT}/convert`,
        {
          refID: refID,
          presignedUrl: presignedUrl,
        }
      );
    }catch (err) {
      console.error("Error posting to encoder:", err);
      return res.status(500).send("Failed to process analysis result.");
    }
  }

  // You could also notify frontend here (e.g., via WebSocket or polling)
  broadcast({
    event: "analysisResult",
    msg: `Analysis result for ${objectName}: ${passed ? "PASSED" : "FAILED"}`,
    report,
    refID: refID, // Include refID in the broadcast
  });
  res.sendStatus(200);
});

// Health check endpoint
app.get("/health", (req, res) => {
  res.send("OK");
});


server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
