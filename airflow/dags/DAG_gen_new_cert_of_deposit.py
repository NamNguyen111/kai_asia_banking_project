import sys
sys.path.append('/opt/airflow/utils')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from gen_new_certificate_of_deposit import insert_mock_cert_of_deposit_holdings

default_args = {
    'description': 'A DAG to insert a new cert of deposit holdings record to raw.certificate_of_deposit_holdings',
    'start_date': datetime(2025, 7, 17),
    'catchup': False,
}

dag = DAG(
    dag_id='insert_new_cert_of_deposit_holdings',
    default_args=default_args,
    schedule=timedelta(minutes=5)
)


with dag:
    task1 = PythonOperator(
        task_id='insert_cert_of_deposit_holdings',
        python_callable=insert_mock_cert_of_deposit_holdings,
        op_kwargs={'n': 3}
    )
    task1