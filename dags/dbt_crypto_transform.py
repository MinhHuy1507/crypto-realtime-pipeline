from airflow import DAG
from datetime import datetime, timedelta

from config.dbt import PROJECT_CONFIG, PROFILE_CONFIG, EXECUTION_CONFIG, RENDER_CONFIG
from cosmos import DbtTaskGroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dbt_crypto_transform',
    default_args=default_args,
    description='DBT DAG to transform crypto data into gold layer, every 1 minute',
    schedule='*/1 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'crypto', 'transformation', 'medallion architecture', 'postgresql', 'minio'],
) as dag:
    dbt_ops = DbtTaskGroup(
        group_id="dbt_transform_group",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RENDER_CONFIG,
        operator_args={
            "install_deps": False,
            "full_refresh": False
        }
    )

    dbt_ops