# for testing snowflake connection
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Default arguments for the DAG
default_args = {
    'owner': 'avahan',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'snowflake_test_dag',
    default_args=default_args,
    description='test dag for snowflake connection',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=['snowflake'],
)

def test_snowflake_connection():
    """Test Snowflake connection"""
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_spotify')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Test a simple query
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"✅ Snowflake connection successful! Version: {version[0]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Snowflake connection failed: {e}")
        return False
    
# Task 4.2: Test if the snowflake connection is working
test_snowflake_conn_task = PythonOperator(
    task_id='test_snowflake_connection',
    python_callable=test_snowflake_connection,
    dag=dag,
)

test_snowflake_conn_task