# utils/logger_config.py
import logging
import os
from datetime import datetime

# def setup_dag_run_logger(dag_id, run_id):
#     """
#     Setup logging configuration for each DAG run
#     Returns a logger that all tasks in the same run can use
#     """
#     # Create logs directory if it doesn't exist
#     os.makedirs('logs/custom', exist_ok=True)
    
#     # Create a daily log file (same file for all runs in a day)
#     today_date = datetime.now().strftime('%Y-%m-%d')
#     log_filename = f'logs/custom/etl_spotify_{today_date}.log'
    
#     # Create logger with a consistent name for the DAG run
#     logger_name = f'etl_{dag_id}_{run_id}'
#     logger = logging.getLogger(logger_name)
#     logger.setLevel(logging.INFO)
    
#     # Avoid adding handlers multiple times
#     if not logger.handlers:

#         # File handler - use 'a' mode to append to the same file throughout the day
#         file_handler = logging.FileHandler(log_filename, encoding='utf-8', mode='a')
#         file_handler.setLevel(logging.INFO)
        
#         # Formatter
#         formatter = logging.Formatter(
#             '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
#             datefmt='%Y-%m-%d %H:%M:%S'
#         )
#         file_handler.setFormatter(formatter)
        
#         logger.addHandler(file_handler)
    
#     return logger

def setup_dag_run_logger(dag_id, run_id):
    """
    Setup a shared daily logger for all DAG runs
    """
    # Create logs directory if it doesn't exist
    os.makedirs('logs/custom', exist_ok=True)
    
    # Create a daily log file
    today_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f'logs/custom/etl_spotify_{today_date}.log'
    
    # Use a consistent logger name for the day
    logger_name = f'etl_spotify_daily'
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Avoid adding handlers multiple times
    if not logger.handlers:
        file_handler = logging.FileHandler(log_filename, encoding='utf-8', mode='a')
        file_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
    
    return logger