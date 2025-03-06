import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts.load_to_gold import move_to_gold

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow-1',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'End-to-end-ETL-scd2',
    default_args=default_args,
    description='A simple PySpark project with Airflow and Medallion architecture',
    schedule_interval=None,  # No automatic scheduling; trigger manually
    start_date=datetime(2024, 12, 10),
    catchup=False,
)

# Define the tasks

# Task 1: Read data (now reads manually defined data)
load_incremental = BashOperator(
    task_id='load_incremental_data_in_source',
    bash_command="python3 /home/barnita/work/airflow-projects/dags/project-3/scripts/incremental.py",
    dag=dag,
)

# Task 2: Transform data
load_to_bronze = BashOperator(
    task_id='load_incremental_data_in_bronze',
    bash_command="python3 /home/barnita/work/airflow-projects/dags/project-3/scripts/load_to_bronze.py",
    dag=dag,
)

# Task 3: Write data
load_to_silver = BashOperator(
    task_id='load_to_silver',
    bash_command="python3 /home/barnita/work/airflow-projects/dags/project-3/scripts/load_to_silver.py",
    dag=dag,
)


load_to_gold = PythonOperator(
    task_id='load_to_gold',
    python_callable=move_to_gold,
    dag=dag,
)

# Task dependencies
load_incremental >> load_to_bronze >> load_to_silver >> load_to_gold

