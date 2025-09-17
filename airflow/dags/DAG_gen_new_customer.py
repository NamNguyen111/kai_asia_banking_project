import sys
sys.path.append('/opt/airflow/utils')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from gen_new_customer import insert_mock_customers

default_args = {
    'description': 'A DAG to insert a new customer record to raw.customers',
    'start_date': datetime(2025, 7, 17),
    'catchup': False,
}

dag = DAG(
    dag_id='insert_new_customer',
    default_args=default_args,
    schedule=timedelta(minutes=1)
)


with dag:
    task1 = PythonOperator(
        task_id='insert_customer',
        python_callable=insert_mock_customers,
        op_kwargs={'n': 100}
    )
    task1