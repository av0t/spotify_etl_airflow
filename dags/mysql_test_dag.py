from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
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
    'mysql_test_dag',
    default_args=default_args,
    description='dag for testing mysql connection',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 9, 28),
    catchup=False,
    tags=['coffee', 'demo', 'mysql'],
)

def test_mysql_connection():
    """Test MySQL connection"""
    try:
        
        hook = MySqlHook(mysql_conn_id='mysql_spotify')
        conn = hook.get_conn()
        print("✅ MySQL connection successful!")
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"MySQL Version: {version[0]}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        return False

test_conn_task = PythonOperator(
    task_id='test_mysql_connection',
    python_callable=test_mysql_connection,
    dag=dag,
)