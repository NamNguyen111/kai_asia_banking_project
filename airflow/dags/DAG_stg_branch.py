import sys
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount



default_args = {
    'description':'A DAG to orchestrate stg_branches data',
    'start_date': datetime(2025, 7, 21),
    'catchup':False, 
}

dag = DAG(
    dag_id='DAG_stg_branches',
    default_args = default_args,
    schedule=timedelta(minutes = 10)

)

with dag:
    task1 = DockerOperator(
        task_id = 'stg_branches',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'docker-compose run --rm dbt dbt run --select stg_branches',
        working_dir = '/user/app',
        mounts = [
            Mount(source='/home/nam11linux/repos/kaiasia_banking_project/dbt/kai_asia_banking_dbt_project/kai_asia_banking_dbt_project',
                target = '/user/app',
                type = 'bind'
            ),
            Mount(source='/home/nam11linux/repos/kaiasia_banking_project/dbt/profiles.yml',
                target = '/root/.dbt/profiles.yml',
                type = 'bind'
            )
        ],
        network_mode = 'kaiasia_banking_project_kaiasia-banking-project-network',
        docker_url = 'unix://var/run/docker.sock',
        auto_remove = 'success'
    )
    task1
