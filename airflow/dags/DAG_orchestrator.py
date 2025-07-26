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
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# Cấu hình profile cho PostgreSQL
profile_config = ProfileConfig(
    profile_name="kai_asia_banking_dbt_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_kai_asia_banking_project",
        profile_args={
            "schema": "staging",
            "dbname": "db_banking",
        },
    ),
)
 
# Cấu hình thực thi
execution_config = ExecutionConfig(
    invocation_mode=InvocationMode.SUBPROCESS,
)

# Default arguments cho DAG
default_args = {
    'owner': 'nam',
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
        task_id="start_staging_pipeline",
        doc_md="Start of the staging data pipeline"
    )










    # Task group chính để chạy tất cả staging models
    staging_models = DbtTaskGroup(
        group_id="staging_models",
        project_config=ProjectConfig(
            dbt_project_path=(DBT_ROOT_PATH / "kai_asia_banking_dbt_project" / "kai_asia_banking_dbt_project").as_posix(),
            # Có thể thêm dbt_vars nếu cần
            # dbt_vars={"env": "staging", "batch_date": "{{ ds }}"}
        ),
        render_config=RenderConfig(
            # Chỉ chạy models trong thư mục staging
            select=["path:models/staging"],
            # select=["tag:staging"],
            enable_mock_profile=False, # Disable mock profile để sử dụng connection thực
            env_vars={  # Biến môi trường nếu cần
                "DBT_ENV": "staging",       
                "BATCH_DATE": "{{ ds }}",   # Airflow execution date
            },
            airflow_vars_to_purge_dbt_ls_cache=["dbt_staging_refresh"], # Cache configuration
        ),
        execution_config=execution_config,
        profile_config=profile_config,
        # Operator arguments
        operator_args={
            "install_deps": True,  # Tự động install dbt dependencies
            "full_refresh": False,  # Không full refresh mặc định
            "vars": {
                "execution_date": "{{ ds }}",
                "dag_run_id": "{{ dag_run.run_id }}",
            }
        },
        # Default args cho các tasks trong group
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=3),
            "execution_timeout": timedelta(hours=2),  # Timeout sau 2 giờ
        },
    )
    


    snapshot_models = DbtTaskGroup(
        group_id = "snapshot_models",
        project_config = ProjectConfig(
            dbt_project_path = (DBT_ROOT_PATH / "kai_asia_banking_dbt_project" / "kai_asia_banking_dbt_project").as_posix(),
        ),
        render_config = RenderConfig(
            select = ["path:snapshots"],
            enable_mock_profile = False,
            env_vars={  # Biến môi trường nếu cần
                "DBT_ENV": "snapshots",       
                "BATCH_DATE": "{{ ds }}",   # Airflow execution date
            },
            airflow_vars_to_purge_dbt_ls_cache=["dbt_staging_refresh"],

        ),
        execution_config=execution_config,
        profile_config=profile_config,
        operator_args={
            "install_deps": True,  # Tự động install dbt dependencies
            "full_refresh": False,  # Không full refresh mặc định
            "vars": {
                "execution_date": "{{ ds }}",
                "dag_run_id": "{{ dag_run.run_id }}",
            }
        },
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=3),
            "execution_timeout": timedelta(hours=2),  # Timeout sau 2 giờ
        },
    )




























    # Task kết thúc
    end_staging = EmptyOperator(
        task_id="end_staging_pipeline",
        doc_md="End of the staging data pipeline"
    )
    start_staging >> staging_models >> snapshot_models >> end_staging



