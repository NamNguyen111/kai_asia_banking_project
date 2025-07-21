import sys
sys.path.append('/opt/airflow/utils')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from gen_new_branch import insert_mock_branches

default_args = {
    'description': 'A DAG to insert a new branch record to raw.branches',
    'start_date': datetime(2025, 7, 17),
    'catchup': False,
}

dag = DAG(
    dag_id='insert_new_branch',
    default_args=default_args,
    schedule=timedelta(minutes=2)
)


with dag:
    task1 = PythonOperator(
        task_id='insert_branch',
        python_callable=insert_mock_branches
    )
    task1