from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

from utils.postgres_ops import check_postgres_connection
from utils.s3_ops import check_s3_connection
from utils.kafka_ops import check_kafka_connection

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ops_check_connectivity',
    description='Check connectivity to Postgres, Minio S3, Spark, and Kafka',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=['/opt/airflow'],
    tags=['ops', 'setup', 'check connections']
) as dag:
    ping_postgres = PythonOperator(
        task_id='ping_postgres',
        python_callable=check_postgres_connection
    )

    ping_minio = PythonOperator(
        task_id='ping_minio',
        python_callable=check_s3_connection
    )

    ping_spark = SparkSubmitOperator(
        task_id='ping_spark',
        conn_id='SPARK_DEFAULT',
        application='spark_jobs/tests/check_spark_connection.py',
        conf={
            'spark.executor.memory': '512m',
            'spark.executor.cores': '2',
            'spark.driver.memory': '512m',
        }
    )

    ping_kafka = PythonOperator(
        task_id='ping_kafka',
        python_callable=check_kafka_connection
    )