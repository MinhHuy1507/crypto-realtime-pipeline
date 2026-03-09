"""
Main pipeline DAG — runs Binance Producer and Spark Streaming jobs.
Prerequisites: ops_init_infrastructure must have been triggered first.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

from utils.kafka_ops import check_kafka_topic_exists
from utils.s3_ops import create_s3_bucket, check_s3_bucket_exists
from config.spark import SPARK_SUBMIT, SPARK_MASTER, JARS

from producers.main import crypto_trades_producer

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id = 'crypto_trades_realtime',
    default_args = default_args,
    schedule = None,
    tags = ['crypto', 'realtime', 'kafka', 'spark', 'minio', 'postgresql'],
    template_searchpath = '/opt/airflow'
) as dag:

    # Task check if topic and bucket were available
    check_bucket_exists = PythonOperator(
        task_id='check_minio_bucket_exists',
        python_callable=check_s3_bucket_exists,
        op_args=['crypto-data']
    )
    check_topic_exists = PythonOperator(
        task_id='check_kafka_topic_exists',
        python_callable=check_kafka_topic_exists,
        op_kwargs={
            'topic_name': 'crypto_trades'
        }
    )

    # Producer
    produce_to_kafka = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=crypto_trades_producer
    )
    # Spark jobs PostgreSQL
    ingest_postgres = BashOperator(
        task_id='ingest_postgres',
        bash_command=f"""
            {SPARK_SUBMIT} \
            --master {SPARK_MASTER} \
            --name "RawIngestion" \
            --jars {JARS} \
            --conf "spark.driver.extraJavaOptions=-javaagent:/opt/jmx_prometheus_javaagent-0.19.0.jar=9101:/opt/airflow/monitoring/spark_jmx/spark-jmx-config.yml" \
            --conf "spark.executor.extraJavaOptions=-javaagent:/opt/jmx_prometheus_javaagent-0.19.0.jar=9102:/opt/airflow/monitoring/spark_jmx/spark-jmx-config.yml" \
            --conf "spark.metrics.conf.*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink" \
            /opt/airflow/spark_jobs/jobs/ingest_postgres.py
        """
    )

    # Define task dependencies
    check_topic_exists >> check_bucket_exists >> [produce_to_kafka, ingest_postgres]