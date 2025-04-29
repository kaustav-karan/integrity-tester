const Minio = require("minio");
require("dotenv").config();

function MinioClient() {
  return new Minio.Client({
    endPoint: process.env.MINIO_ENDPOINT || "localhost",
    port: 9000,
    useSSL: false,
    accessKey: process.env.MINIO_ACCESS_KEY || "minioadmin",
    secretKey: process.env.MINIO_SECRET || "minioadmin",
  });
}

module.exports = { MinioClient };
