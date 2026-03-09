"""
One-time infrastructure setup DAG — creates PostgreSQL schemas, Kafka topics, and MinIO buckets.
All operations are idempotent (safe to re-run).
"""
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.kafka_ops import create_kafka_topic
from utils.s3_ops import create_s3_bucket

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id = 'ops_init_infrastructure',
    description = 'Initialize infrastructure: Create Postgres raw schema, Kafka topics, and MinIO buckets',
    default_args = default_args,
    schedule = None,
    tags = ['ops', 'setup', 'infrastructure'],
    template_searchpath = ['/opt/airflow']
) as dag:
    create_raw_schema_tables = SQLExecuteQueryOperator(
        task_id='create_raw_schema_tables',
        conn_id='POSTGRES_DW',
        sql='producers/sql/ddl_raw.sql'
    )

    create_kafka_topics = PythonOperator(
        task_id='create_kafka_topic',
        python_callable=create_kafka_topic,
        op_kwargs={'topic_name': 'crypto_trades', 'num_partitions': 2, 'replication_factor': 1}
    )

    create_minio_buckets = PythonOperator(
        task_id='create_minio_bucket',
        python_callable=create_s3_bucket,
        op_args=['crypto-data']
    )