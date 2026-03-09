import os

POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")

# PostgreSQL configuration for Airflow PostgresHook
POSTGRES_CONN_ID = 'POSTGRES_DW'