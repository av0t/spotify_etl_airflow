from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 25),
    "email": ["avahant@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,  # Optional: set to True if you want retry notifications
}

# Define the DAG
dag = DAG(
    'email_test_dag',
    default_args=default_args,
    description='DAG for testing email on failure and success',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 9, 25),
    catchup=False,  # Don't run historical instances
)

# Task that will succeed - tests EmailOperator
test_email_success = EmailOperator(
    task_id='email_success_test',
    to="avahant@gmail.com",
    subject="Airflow Email Test - Success",
    html_content="""<h1>Testing email using airflow - This task succeeded!</h1>""",
    dag=dag
)

# Task that will fail - tests email_on_failure
test_failure_task = BashOperator(
    task_id='intentional_failure',
    bash_command='exit 1',  # This will always fail
    dag=dag
)

# Set task dependencies (optional - they can run in parallel)
test_email_success 
test_failure_task