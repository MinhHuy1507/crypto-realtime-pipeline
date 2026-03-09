import os

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-data")
S3_OUTPUT_PATH = f"s3a://{S3_BUCKET_NAME}/trades"
S3_CHECKPOINT_DIR = os.getenv("S3_CHECKPOINT_DIR", f"s3a://{S3_BUCKET_NAME}/checkpoints")

# S3 configuration for Airflow S3Hook
AWS_CONN_ID = 'MINIO_S3'