"""
Spotify data processing DAG
Integrates spotify ETL project into Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from helpers.extract import extract_spotify_data
from helpers.transform import transform_spotify_data
from helpers.load import load_spotify_data, load_to_snowflake, log_etl_data

# Default arguments for the DAG
default_args = {
    'owner': 'avahan',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'spotify_data_pipeline',
    default_args=default_args,
    description='Spotify data pipeline from spotify API to local MySQL installation and Snowflake',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=['spotify'],
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

# Task 1: Extract spotify data from api and save to .csv file
extract_task = PythonOperator(
    task_id='extract_spotify_data',
    python_callable=extract_spotify_data,
    dag=dag,
)

# Task 2: File Sensor - Wait for file to arrive
wait_for_Efile_sensor = FileSensor(
    task_id='wait_for_extract_file',
    filepath='/tmp/spotify_extracted/spotify_data.csv',
    poke_interval=10,         # Check every 10 seconds
    timeout=300,              # Timeout after 5 minutes
    dag=dag,
)

# Task 3.1: Transform spotify data after extraction and save to .csv file
transform_task = PythonOperator(
    task_id='transform_spotify_data',
    python_callable=transform_spotify_data,
    dag=dag,
)

# Task 3.2: File Sensor - Wait for file to arrive
wait_for_Tfile_sensor = FileSensor(
    task_id='wait_for_transform_file',
    filepath='/tmp/spotify_transformed/spotify_data_tran.csv',
    poke_interval=10,         # Check every 10 seconds
    timeout=300,              # Timeout after 5 minutes
    dag=dag,
)

# Task 4.1: Test if the MySQL connection is working
test_conn_task = PythonOperator(
    task_id='test_mysql_connection',
    python_callable=test_mysql_connection,
    dag=dag,
)

# Task 4.2: Test if the snowflake connection is working
test_snowflake_conn_task = PythonOperator(
    task_id='test_snowflake_connection',
    python_callable=test_snowflake_connection,
    dag=dag,
)

# Task 5.1: Create table in MySQL
create_mysql_table = MySqlOperator(
    task_id='create_mysql_table',
    mysql_conn_id='mysql_spotify',
    sql='''
    CREATE TABLE IF NOT EXISTS tracks (
    id VARCHAR(255) PRIMARY KEY,
    track_name TEXT,
    artist_names TEXT,
    artist_count INT,
    release_date DATE,
    duration_min FLOAT,
    popularity INT,
    cover_image_url TEXT,
    album_type VARCHAR(50),
    total_streams BIGINT,
    danceability INT,
    tempo FLOAT
    );
    ''',
    dag=dag,
)

# Task 5.2: Create table in snowflake
# create_snowflake_table = SnowflakeOperator(
#     task_id='create_snowflake_table',
#     mysql_conn_id='snowflake_spotify',
#     sql='''
#     CREATE TABLE IF NOT EXISTS tracks (
#     id VARCHAR(255) PRIMARY KEY,
#     track_name TEXT,
#     artist_names TEXT,
#     artist_count INT,
#     release_date DATE,
#     duration_min FLOAT,
#     popularity INT,
#     cover_image_url TEXT,
#     album_type VARCHAR(50),
#     total_streams BIGINT,
#     danceability INT,
#     tempo FLOAT
#     );
#     ''',
#     dag=dag,
# )

# Task 5.3: Create log table
create_log_table = MySqlOperator(
    task_id='create_log_table',
    mysql_conn_id='mysql_spotify',
    sql='''
    CREATE TABLE IF NOT EXISTS etl_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    status ENUM('success', 'failure') NOT NULL,
    extract_status ENUM('success', 'failure'),
    transform_status ENUM('success', 'failure'),
    load_status ENUM('success', 'failure'),
    tracks_extracted INT,
    tracks_loaded INT,
    error_message TEXT,
    snow_tracks_loaded INT,
    snow_load_status ENUM('success', 'failure'),
    snow_error_message TEXT
    );
    ''',
    dag=dag,
)

# Task 6: Load transformed data into MySQL installed on host machine
load_task = PythonOperator(
    task_id='load_spotify_data',
    python_callable=load_spotify_data,
    dag=dag,
)

# Task 7: Load transformed data into Snowflake 
load_snowflake_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)

# Task 8: Log ETL data into log table present in MySQL
log_task = PythonOperator(
    task_id='log_etl_data',
    python_callable=log_etl_data,
    dag=dag,
)

# setting dependencies

extract_task >> wait_for_Efile_sensor >> [transform_task, test_conn_task, test_snowflake_conn_task]

test_conn_task >> create_mysql_table >> create_log_table

transform_task >> wait_for_Tfile_sensor

[create_mysql_table, wait_for_Tfile_sensor] >> load_task

[test_snowflake_conn_task, wait_for_Tfile_sensor] >> load_snowflake_task

[create_log_table, load_task, load_snowflake_task] >> log_task