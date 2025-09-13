"""
DAG to run all snapshots for Kai Asia Banking Project
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.operators.local import DbtSnapshotLocalOperator, DbtRunLocalOperator
from cosmos.constants import InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Cấu hình đường dẫn dbt project
# Trong container, dbt được mount tại /opt/airflow/dbt
DEFAULT_DBT_ROOT_PATH = Path("/opt/airflow/dbt")
# DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

base_profile_config = ProfileConfig(
    profile_name="kai_asia_banking_snapshot_profile",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_kai_asia_banking_project",
        profile_args={
            "dbname": "db_banking",
            "schema": "public", # Dummy schema để đỡ phải tạo nhiều profile
        },
    ),
)

# Cấu hình thực thi
execution_config = ExecutionConfig(
    invocation_mode=InvocationMode.SUBPROCESS,
)

# Default arguments cho DAG
default_args = {
    'owner': 'nam_11',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

with DAG(
    dag_id="dbt_snapshots_kai_asia_banking",
    default_args=default_args,
    description="Run all snapshots for Kai Asia Banking Project",
    schedule = "0 19 * * *",  # 19h UTC = 2h sáng VN
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "snapshot_orchestrator", "kai_asia_banking"],
) as dag:
    
    # Task khởi đầu
    start_snapshot = EmptyOperator(
        task_id="start_point_of_the_pipeline",
        doc_md="Start of the data pipeline"
    )


    snp_branches = DockerOperator(
        task_id = 'snapshot_branches_run_by_docker_operator',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'snapshot --select snp_branches',
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

    snp_customers = DockerOperator(
        task_id = 'snapshot_customers_run_by_docker_operator',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'snapshot --select snp_customers',
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

    snp_accounts = DockerOperator(
        task_id = 'snapshot_accounts_run_by_docker_operator',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'snapshot --select snp_accounts',
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

    snp_bonds = DockerOperator(
        task_id = 'snapshot_bonds_run_by_docker_operator',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'snapshot --select snp_bonds',
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

    snp_loans = DockerOperator(
        task_id = 'snapshot_loans_run_by_docker_operator',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'snapshot --select snp_loans',
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

    # Task kết thúc
    end_snapshot = EmptyOperator(
        task_id="end_point_of_the_pipeline",
        doc_md="End of the data pipeline"
    )
    start_snapshot >> [snp_branches, snp_customers, snp_accounts, snp_bonds, snp_loans] >> end_snapshot



