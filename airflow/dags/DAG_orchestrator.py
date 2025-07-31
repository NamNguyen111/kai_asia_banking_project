"""
DAG to run all dbt models for Kai Asia Banking Project
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
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
    profile_name="kai_asia_banking_dbt_project",
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
    dag_id="dbt_all_models_kai_asia_banking",
    default_args=default_args,
    description="Run all dbt models for Kai Asia Banking Project",
    schedule="@daily",  # Chạy hàng ngày lúc 00:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "orchestrator", "kai_asia_banking"],
) as dag:
    
    # Task khởi đầu
    start_staging = EmptyOperator(
        task_id="start_point_of_the_pipeline",
        doc_md="Start of the data pipeline"
    )



    # Task group chính để chạy tất cả staging models
    staging_models = DbtTaskGroup(
        group_id="staging_models",
        project_config=ProjectConfig(
            dbt_project_path=(DEFAULT_DBT_ROOT_PATH / "kai_asia_banking_dbt_project" / "kai_asia_banking_dbt_project").as_posix(),
            # Có thể thêm dbt_vars nếu cần
            # dbt_vars={"env": "staging", "batch_date": "{{ ds }}"}
        ),
        render_config=RenderConfig(
            # Chỉ chạy models trong thư mục staging
            # select=["path:models/staging"],
            select=["tag:staging"],
            enable_mock_profile=False, # Disable mock profile để sử dụng connection thực
            env_vars={  # Biến môi trường nếu cần
                "DBT_ENV": "staging",       
                "BATCH_DATE": "{{ ds }}",   # Airflow execution date
            },
            # airflow_vars_to_purge_dbt_ls_cache=["dbt_staging_refresh"], # Cache configuration
        ),
        execution_config=execution_config,
        profile_config=base_profile_config,
        # Operator arguments
        operator_args={
            "vars": {"custom_schema": "staging"},
            "install_deps": True,  # Tự động install dbt dependencies
            "full_refresh": False,  # Không full refresh mặc định
            # "vars": {
            #     "execution_date": "{{ ds }}",
            #     "dag_run_id": "{{ dag_run.run_id }}",
            # }
        },
        # Default args cho các tasks trong group
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=3),
            "execution_timeout": timedelta(hours=2),  # Timeout sau 2 giờ
        },
    )
    

    task1 = DockerOperator(
        task_id = 'snapshot_models_run_by_docker_operator',
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




























    # Task kết thúc
    end_staging = EmptyOperator(
        task_id="end_point_of_the_pipeline",
        doc_md="End of the data pipeline"
    )
    start_staging >> staging_models >> task1 >> end_staging



