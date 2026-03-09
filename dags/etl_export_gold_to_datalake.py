from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.postgres_ops import check_postgres_connection, check_postgres_schema_exists, ingest_postgres_to_s3_all_tables_in_schema


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id = 'etl_export_gold_to_datalake',
    default_args = default_args,
    schedule = '@daily',
    template_searchpath = '/opt/airflow'
) as dag:
    check_postgres_conn = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection
    )
    check_schema_gold = PythonOperator(
        task_id='check_postgres_schema_exists_gold',
        python_callable=check_postgres_schema_exists,
        op_args=['gold']
    )

    # Define ingestion tasks for each schema
    # Using Airflow Macros for Daily Partitioning (Snapshot Strategy)
    partition_suffix = "dt={{ ds }}"

    ingest_gold = PythonOperator(
        task_id='ingest_postgres_to_s3_gold',
        python_callable=ingest_postgres_to_s3_all_tables_in_schema,
        op_kwargs={
            'schema_name': 'gold', 
            'bucket_name': 'crypto-data', 
            's3_prefix': f'dbt/{partition_suffix}/',
            'date_column': 'trade_date',
            'target_date': '{{ ds }}'
        }
    )

    # Define task dependencies
    check_postgres_conn >> check_schema_gold >> ingest_gold