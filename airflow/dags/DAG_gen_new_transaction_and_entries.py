import sys
sys.path.append('/opt/airflow/utils')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import uuid
import random
import psycopg2
from faker import Faker
from gen_new_transaction_and_entries import navigate_func

default_args = {
    'description': 'A DAG to insert a new transaction and its entries to raw',
    'start_date': datetime(2025, 7, 17),
    'catchup': False,
}

dag = DAG(
    dag_id='insert_new_transaction_and_entries',
    default_args=default_args,
    schedule=timedelta(minutes=5)
)

with dag:
    task1 = PythonOperator(
        task_id='insert_new_transaction_and_entries',
        python_callable=navigate_func
    )
    task1