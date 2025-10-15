from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
import os

# Default arguments for the DAG
default_args = {
    'owner': 'avahan',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'pg_test_dag',
    default_args=default_args,
    description='dag for testing pg connection',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 9, 24),
    catchup=False,
    tags=['coffee', 'demo', 'postgres'],
)

def test_postgres_connection():
    """Test if we can connect to PostgreSQL"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_coffee')
        conn = hook.get_conn()
        print("✅ PostgreSQL connection successful!")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

test_conn_task = PythonOperator(
    task_id='test_postgres_connection',
    python_callable=test_postgres_connection,
    dag=dag,
)