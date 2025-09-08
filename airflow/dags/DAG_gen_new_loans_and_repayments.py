import sys
sys.path.append('/opt/airflow/utils')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from gen_new_loans import insert_mock_loans

default_args = {
    'description': 'A DAG to insert a new loans raw.loans',
    'start_date': datetime(2025, 7, 17),
    'catchup': False,
}

dag = DAG(
    dag_id='insert_new_loans',
    default_args=default_args,
    schedule=timedelta(minutes=5)
)


with dag:
    task1 = PythonOperator(
        task_id='insert_loans',
        python_callable=insert_mock_loans,
        op_kwargs={'n': 1}
    )
    task1