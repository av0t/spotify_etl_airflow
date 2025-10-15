# helper file for loading dataset into local installation of MySQL on host machine

# import mysql.connector
# from mysql.connector import Error
import os
from dotenv import load_dotenv
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from helpers.logger_config import setup_dag_run_logger


def load_spotify_data(**context):
    """
    Load task for Airflow - Incremental Loads the transformed data into local installation of MySQL on host machine
    """
    # Get file path from XCom (from create_coffee_data task)
    file_path = context['task_instance'].xcom_pull(task_ids='transform_spotify_data', key='file_path')
    etl_run_data = context['task_instance'].xcom_pull(task_ids='transform_spotify_data', key='etl_run_data')
    
    # Get logger for this DAG run
    logger = setup_dag_run_logger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )

    # Read the CSV file
    df = pd.read_csv(file_path)
    print("Data loaded: {} rows".format(len(df)))

    print("=== Storing Data in MySQL ===")
    logger.info("=== Storing Data in MySQL ===")

    # Get MySQL connection using MySQL Hook
    mysql_hook = MySqlHook(mysql_conn_id='mysql_spotify')
    
    try:
        # Get current count before insertion
        count_before = mysql_hook.get_records("SELECT COUNT(*) FROM tracks")[0][0]
        print(f"✓ Current records in database: {count_before}")
        logger.info(f"Current records in database: {count_before}")
        
        # Define target fields (columns in the table)
        target_fields = ['id', 'track_name', 'artist_names', 'artist_count', 'release_date', 
                        'duration_min', 'popularity', 'cover_image_url', 'album_type', 
                        'total_streams', 'danceability', 'tempo']
        
        # Convert DataFrame to list of tuples
        records = [tuple(row) for row in df[target_fields].itertuples(index=False)]
        
        # Insert data - let MySQL handle duplicates via table constraints
        mysql_hook.insert_rows(
            table='tracks',
            rows=records,
            target_fields=target_fields,
            replace=True  # This will replace on duplicate key () -- essentially ignoring row if primary key is already present
        )
        
        # Get count after insertion
        count_after = mysql_hook.get_records("SELECT COUNT(*) FROM tracks")[0][0]
        print(f"✓ Total records in database: {count_after}")
        logger.info(f"Total records in database after loading: {count_after}")
        
        inserted_count = count_after - count_before
        print(f"✓ Successfully inserted {inserted_count} new records into MySQL!")
        logger.info(f"Successfully inserted {inserted_count} new records into MySQL!")

        etl_run_data['tracks_loaded'] = inserted_count
        etl_run_data['load_status'] = 'success'
        etl_run_data['status'] = 'success'
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='loaded_rows', value=inserted_count)
        context['task_instance'].xcom_push(key='etl_run_data', value=etl_run_data)

        
        return f"Successfully loaded {inserted_count} new records"
        
    except Exception as e:
        print(f"✗ Error loading data to MySQL: {e}")
        logger.error(f"Error loading data to MySQL: {e}")
        etl_run_data['error_message'] = e
        raise e

def load_to_snowflake(**context):
    """
    Load task for Airflow - Incremental Loads the transformed data into Snowflake
    """
    # Get file path from XCom
    file_path = context['task_instance'].xcom_pull(task_ids='transform_spotify_data', key='file_path')
    #etl_run_data = context['ti'].xcom_pull(task_ids='transform_spotify_data', key='etl_run_data')
    
    # Get logger for this DAG run
    logger = setup_dag_run_logger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )

    # Read the CSV file
    df = pd.read_csv(file_path)
    print("Data loaded for Snowflake: {} rows".format(len(df)))
    logger.info(f"Data loaded for Snowflake: {len(df)} rows")

    print("=== Storing Data in Snowflake ===")
    logger.info("=== Storing Data in Snowflake ===")

    # Get Snowflake connection using Snowflake Hook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_spotify')
    
    try:
        # Get a single connection for all operations
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists (similar to MySQL structure)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS tracks (
            id VARCHAR(255) PRIMARY KEY,
            track_name TEXT,
            artist_names TEXT,
            artist_count INTEGER,
            release_date DATE,
            duration_min FLOAT,
            popularity INTEGER,
            cover_image_url TEXT,
            album_type VARCHAR(50),
            total_streams BIGINT,
            danceability INTEGER,
            tempo FLOAT
        )
        """
        
        cursor.execute(create_table_sql)
        logger.info("✓ Snowflake table 'tracks' created/verified")
        
        # Get current count before insertion
        cursor.execute("SELECT COUNT(*) FROM tracks")
        count_before = cursor.fetchone()[0]
        print(f"✓ Current records in Snowflake: {count_before}")
        logger.info(f"Current records in Snowflake: {count_before}")
        
        # Define target fields (columns in the table) - same as MySQL
        target_fields = ['id', 'track_name', 'artist_names', 'artist_count', 'release_date', 
                        'duration_min', 'popularity', 'cover_image_url', 'album_type', 
                        'total_streams', 'danceability', 'tempo']
        
        # Convert DataFrame to list of tuples
        records = [tuple(row) for row in df[target_fields].itertuples(index=False)]
        
        # Create temporary table for batch processing (using the same connection)
        create_temp_table_sql = """
        CREATE OR REPLACE TEMPORARY TABLE temp_tracks (
            id VARCHAR(255),
            track_name TEXT,
            artist_names TEXT,
            artist_count INTEGER,
            release_date DATE,
            duration_min FLOAT,
            popularity INTEGER,
            cover_image_url TEXT,
            album_type VARCHAR(50),
            total_streams BIGINT,
            danceability INTEGER,
            tempo FLOAT
        )
        """
        cursor.execute(create_temp_table_sql)
        logger.info("✓ Temporary table created in Snowflake")
        
        # Insert all data into temporary table using the same cursor
        insert_temp_sql = """
        INSERT INTO temp_tracks 
        (id, track_name, artist_names, artist_count, release_date, duration_min, 
         popularity, cover_image_url, album_type, total_streams, danceability, tempo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Execute batch insert with the same cursor
        cursor.executemany(insert_temp_sql, records)
        logger.info(f"✓ Inserted {len(records)} records into temporary table")
        
        # Merge data from temporary table to main table (UPSERT)
        merge_sql = """
        MERGE INTO tracks AS target
        USING temp_tracks AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET 
                track_name = source.track_name
        WHEN NOT MATCHED THEN
            INSERT (id, track_name, artist_names, artist_count, release_date, duration_min,
                    popularity, cover_image_url, album_type, total_streams, danceability, tempo)
            VALUES (source.id, source.track_name, source.artist_names, source.artist_count, 
                    source.release_date, source.duration_min, source.popularity, 
                    source.cover_image_url, source.album_type, source.total_streams, 
                    source.danceability, source.tempo)
        """
        
        cursor.execute(merge_sql)
        logger.info("✓ Merged data from temporary to main table")
        
        # Get count after insertion
        cursor.execute("SELECT COUNT(*) FROM tracks")
        count_after = cursor.fetchone()[0]
        print(f"✓ Total records in Snowflake: {count_after}")
        logger.info(f"Total records in Snowflake after loading: {count_after}")
        
        inserted_count = count_after - count_before
        print(f"✓ Successfully inserted {inserted_count} new records into Snowflake!")
        logger.info(f"Successfully inserted {inserted_count} new records into Snowflake!")

        # Commit the transaction
        conn.commit()
        
        # Update ETL run data
        # etl_run_data['snowflake_tracks_loaded'] = inserted_count
        # etl_run_data['snowflake_load_status'] = 'success'
        snow_run_data ={
            'snowflake_tracks_loaded' : inserted_count,
            'snowflake_load_status' : 'success',
            'snowflake_error_message' : None
        }
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='snowflake_loaded_rows', value=inserted_count)
        context['task_instance'].xcom_push(key='snow_run_data', value=snow_run_data)

        return f"Successfully loaded {inserted_count} new records to Snowflake"
        
    except Exception as e:
        # Rollback in case of error
        if 'conn' in locals():
            conn.rollback()
        print(f"✗ Error loading data to Snowflake: {e}")
        logger.error(f"Error loading data to Snowflake: {e}")
        # snow_run_data['snowflake_error_message'] = str(e)
        # snow_run_data['snowflake_load_status'] = 'failure'
        raise e
    finally:
        # Always close the connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def log_etl_data(**context):
    """
    Log ETL data into MySQL installed on host machine
    """

    # Get logger for this DAG run
    logger = setup_dag_run_logger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )

    # Get etl run data from XCom
    etl_run_data = context['task_instance'].xcom_pull(task_ids='load_spotify_data', key='etl_run_data')
    snow_run_data = context['task_instance'].xcom_pull(task_ids='load_to_snowflake', key='snow_run_data')

    # Merge the values into the first dictionary. If same key, second one will get priority in this logic
    etl_run_data.update(snow_run_data)

    if not etl_run_data:
            print("✗ No ETL run data found in XCom")
            return
    
    print("=== Logging ETL Run Data ===")
    logger.info("=== Logging ETL Run Data ===")

    # Get MySQL connection using MySQL Hook
    mysql_hook = MySqlHook(mysql_conn_id='mysql_spotify')
    
    # Define the insert query
    insert_query = """
    INSERT INTO etl_log 
    (status, extract_status, transform_status, load_status, 
     tracks_extracted, tracks_loaded, error_message, snow_tracks_loaded, snow_load_status, snow_error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Prepare the values
    values = (
        etl_run_data['status'],
        etl_run_data['extract_status'],
        etl_run_data['transform_status'],
        etl_run_data['load_status'],
        etl_run_data['tracks_extracted'],
        etl_run_data['tracks_loaded'],
        etl_run_data['error_message'],
        etl_run_data['snowflake_tracks_loaded'],
        etl_run_data['snowflake_load_status'],
        etl_run_data['snowflake_error_message']
    )
    
    # Insert the log record
    mysql_hook.run(insert_query, parameters=values)
    print("✓ ETL run data successfully logged to database")
    logger.info("ETL run data successfully logged to database\n")

    return "ETL logging completed"   

# Testing
if __name__ == "__main__":
    print("Load module ready")
    # query_sample_data(3)