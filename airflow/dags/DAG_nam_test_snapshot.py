"""
DAG to run all dbt models for Kai Asia Banking Project
"""
import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

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
    dag_id="dbt_snapshots_models_kai_asia_banking",
    default_args=default_args,
    description="Run snapshot dbt models for Kai Asia Banking Project",
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
    snapshot_models = DbtTaskGroup(
        group_id = "snapshot_models",
        project_config = ProjectConfig(
            dbt_project_path = (DEFAULT_DBT_ROOT_PATH / "kai_asia_banking_dbt_project" / "kai_asia_banking_dbt_project").as_posix(),
        ),
        render_config = RenderConfig(
            # select=[
            #     "resource_type:snapshot",
            # ],
            select=["snp_branches"],
            enable_mock_profile = False,
            env_vars={  
                "DBT_ENV": "snapshots",       
                "BATCH_DATE": "{{ ds }}",
            },
        ),
        execution_config=execution_config,
        profile_config=base_profile_config,
        operator_args={
            "vars": {"custom_schema": "snapshots"},
            "install_deps": True,  # Tự động install dbt dependencies
            "full_refresh": False,  # Không full refresh mặc định
        },
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=3),
            "execution_timeout": timedelta(hours=2),  # Timeout sau 2 giờ
        },
    )
    # Task kết thúc
    end_staging = EmptyOperator(
        task_id="end_point_of_the_pipeline",
        doc_md="End of the data pipeline"
    )
    start_staging >> snapshot_models >> end_staging



