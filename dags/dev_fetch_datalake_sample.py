from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from utils.s3_ops import download_s3_folder, check_s3_bucket_exists
from config.s3 import S3_BUCKET_NAME
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
LOCAL_PATH = "/opt/airflow/data/datalake"

def _download_task(**context):
    """
    Get parameters from UI and perform download
    """
    params = context['params']
    s3_folder = params['s3_folder']
    bucket_name = params['bucket_name']

    # -> Example Local Path: /opt/airflow/data/datalake/dbt/gold
    local_destination = os.path.join(LOCAL_PATH, s3_folder)
    
    logger.info(f"Starting download from S3 to Local...")
    logger.info(f"Source S3: s3://{bucket_name}/{s3_folder}")
    logger.info(f"Target Local: {local_destination}")
    
    download_s3_folder(
        bucket_name=bucket_name,
        s3_folder=s3_folder,
        local_dir=local_destination
    )
    
    logger.info(f"Download completed. Check your local folder: data/datalake/{s3_folder}")

with DAG(
    dag_id="dev_fetch_datalake_sample",
    description="Manual tool to download Data from S3 to Local for inspection (Dev)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dev", "tool"],
    params={
        "s3_folder": Param(
            default="dbt/dt=2026-02-01", 
            type="string", 
            title="S3 Folder Path",
            description="Input the S3 folder path to download from (e.g., dbt/dt=2026-02-01)"
        ),
        "bucket_name": Param(
            default=S3_BUCKET_NAME,
            type="string",
            title="S3 Bucket Name"
        )
    }
) as dag:
    
    check_bucket = PythonOperator(
        task_id="check_bucket_exists",
        python_callable=check_s3_bucket_exists,
        op_kwargs={
            "bucket_name": "{{ params.bucket_name }}"
        }
    )

    task_download = PythonOperator(
        task_id="perform_download",
        python_callable=_download_task
    )

    check_bucket >> task_download