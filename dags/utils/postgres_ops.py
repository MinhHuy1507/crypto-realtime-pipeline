from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from config.postgres import POSTGRES_CONN_ID
from config.s3 import AWS_CONN_ID

import csv
from io import StringIO
import logging

logger = logging.getLogger(__name__)

def check_postgres_connection():
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT 1;')
        result = cursor.fetchone()
        logger.info(f"Postgres Connection Successful, Query Result: {result}")
    except Exception as e:
        logger.error(f"Postgres Connection Failed: {e}")

def check_postgres_schema_exists(schema_name: str) -> bool:
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);"
        exists = pg_hook.get_first(sql, parameters=(schema_name,))[0]
        logger.info(f"Schema {schema_name} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking if schema exists: {e}")
        return False

def check_postgres_table_exists(table_name: str, schema_name: str = 'public') -> bool:
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """
        exists = pg_hook.get_first(sql, parameters=(schema_name, table_name))[0]
        logger.info(f"Table {schema_name}.{table_name} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        return False

def check_postgres_column_exists(table_name: str, column_name: str, schema_name: str = 'public') -> bool:
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = %s
                AND column_name = %s
            );
        """
        result = pg_hook.get_first(sql, parameters=(schema_name, table_name, column_name))
        return result[0] if result else False
    except Exception as e:
        logger.error(f"Error checking if column exists: {e}")
        return False
    
def list_postgres_tables(schema_name: str):
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}';
        """)
        tables = cursor.fetchall()
        table_list = [table[0] for table in tables]
        logger.info(f"Tables in schema {schema_name}: {table_list}")
        return table_list
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return []

def ingest_postgres_to_s3(schema_name: str, table_name: str, bucket_name: str, s3_key: str, where_clause: str = None):
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        sql_query = f'SELECT * FROM {schema_name}.{table_name}'
        if where_clause:
            sql_query += f' WHERE {where_clause}'
        cursor.execute(sql_query + ';')
        rows = cursor.fetchall()
        if not rows:
            logger.info(f"No data to export for {schema_name}.{table_name} with condition: {where_clause}")
            return 

        # Convert data to CSV format        
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerows(rows)
        
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        logger.info(f"Data from table {schema_name}.{table_name} ingested to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"Error ingesting data to S3: {e}")


def ingest_postgres_to_s3_all_tables_in_schema(schema_name: str, bucket_name: str, s3_prefix: str, date_column: str = None, target_date: str = None):
    try:
        tables = list_postgres_tables(schema_name)
        for table in tables:
            clean_prefix = s3_prefix.rstrip('/')
            s3_key = f"{clean_prefix}/{schema_name}/{table}.csv"
            
            where_condition = None
            
            # Check column existence before applying filter
            if date_column and target_date:
                if check_postgres_column_exists(table, date_column, schema_name):
                    where_condition = f"{date_column}::DATE = '{target_date}'"
                else:
                    logger.info(f"Table {schema_name}.{table} missing column '{date_column}'. Falling back to FULL SNAPSHOT.")

            ingest_postgres_to_s3(schema_name, table, bucket_name, s3_key, where_condition)
    except Exception as e:
        logger.error(f"Error ingesting all tables to S3: {e}")
