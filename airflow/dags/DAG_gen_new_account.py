import sys
sys.path.append('/opt/airflow/utils')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from gen_new_account import insert_mock_accounts

default_args = {
    'description': 'A DAG to insert a new accounts record to raw.accounts',
    'start_date': datetime(2025, 7, 17),
    'catchup': False,
}

dag = DAG(
    dag_id='insert_new_account',
    default_args=default_args,
    schedule=timedelta(minutes=1)
)


with dag:
    task1 = PythonOperator(
        task_id='insert_account',
        python_callable=insert_mock_accounts,
        op_kwargs={'n': 10}
    )
    task1