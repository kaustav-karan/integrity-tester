import redis
import json
import requests
from minio import Minio
from minio.error import S3Error
from checker import run_integrity_checks
import os
import uuid
import config

# Setup clients
redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
minio_client = Minio(
    config.MINIO_ENDPOINT,
    access_key=config.MINIO_ACCESS_KEY,
    secret_key=config.MINIO_SECRET_KEY,
    secure=False  # Change to True if using SSL
)

DOWNLOAD_DIR = 'downloads'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

print("Analyzer is listening for jobs...")

while True:
    _, msg = redis_client.brpop('analysisQueue')
    data = json.loads(msg)
    object_name = data['objectName']
    print(f"Received job: {object_name}")

    # Download from MinIO
    temp_path = os.path.join(DOWNLOAD_DIR, f"{uuid.uuid4()}.wav")
    try:
        minio_client.fget_object(config.BUCKET_NAME, object_name, temp_path)
    except S3Error as e:
        print(f"MinIO error: {e}")
        continue

    # Run analysis
    passed, report = run_integrity_checks(temp_path)
    print(f"Analysis for {object_name}: {'PASSED' if passed else 'FAILED'}")

    # Post result back to Node server
    try:
        response = requests.post(config.NODE_ANALYSIS_RESULT_ENDPOINT, json={
            'objectName': object_name,
            'passed': passed,
            'report': report
        })
        response.raise_for_status()
        print(f"Result posted successfully for {object_name}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to post result for {object_name}: {e}")

    # Clean up
    os.remove(temp_path)
