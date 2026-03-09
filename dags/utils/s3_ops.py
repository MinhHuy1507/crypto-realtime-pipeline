from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from config.s3 import AWS_CONN_ID
import os
import logging

logger = logging.getLogger(__name__)

def check_s3_connection():
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        conn = s3_hook.get_conn()
        logger.info(f"S3 Connection Successful: {conn}")
    except Exception as e:
        logger.error(f"S3 Connection Failed: {e}")

def create_s3_bucket(bucket_name: str):
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        if (s3_hook.check_for_bucket(bucket_name)):
            logger.info(f"Bucket {bucket_name} already exists.")
        else: 
            s3_hook.create_bucket(bucket_name)
            logger.info("Creating S3 bucket successfully")
    except Exception as e:
        logger.error(f"Creating S3 bucket failed: {e}")

def check_s3_bucket_exists(bucket_name: str) -> bool:
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        exists = s3_hook.check_for_bucket(bucket_name)
        logger.info(f"Bucket {bucket_name} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking if bucket exists: {e}")
        return False

def download_s3_folder(bucket_name: str, s3_folder: str, local_dir: str):
    """
    Download all files from an S3 folder (prefix) to a local directory.
    Mirroring the structure relative to the s3_folder.
    """
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=s3_folder)
        
        if not keys:
            logger.info(f"No files found in s3://{bucket_name}/{s3_folder}")
            return

        for key in keys:
            folder_prefix = s3_folder if s3_folder.endswith('/') else s3_folder + '/'
            if key.startswith(folder_prefix):
                relative_path = key.replace(folder_prefix, "", 1)
            else:
                relative_path = os.path.basename(key)
            
            local_file_path = os.path.join(local_dir, relative_path)
            
            # Ensure local directory exists
            local_file_dir = os.path.dirname(local_file_path)
            if not os.path.exists(local_file_dir):
                os.makedirs(local_file_dir)
            
            # Download file
            s3_hook.get_key(key, bucket_name).download_file(local_file_path)
            logger.info(f"Downloaded {key} to {local_file_path}")
            
        logger.info(f"Downloaded folder {s3_folder} from {bucket_name} to {local_dir}")
    except Exception as e:
        logger.error(f"Error downloading folder from S3: {e}")

