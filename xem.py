import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Cấu hình đường dẫn dbt project
DEFAULT_DBT_ROOT_PATH = Path("/opt/airflow/dbt")
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = (DBT_ROOT_PATH / "kai_asia_banking_dbt_project" / "kai_asia_banking_dbt_project").as_posix()

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
    dag_id="dbt_complete_pipeline_kai_asia_banking",
    default_args=default_args,
    description="Complete dbt pipeline with staging, snapshots, and marts",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "complete_pipeline", "kai_asia_banking"],
    doc_md="""
    # Complete dbt Pipeline for Kai Asia Banking
    
    This DAG runs the complete data pipeline:
    1. **Staging Layer**: Raw data transformation and cleaning
    2. **Snapshots**: SCD Type 2 for slowly changing dimensions
    3. **Intermediate Layer**: Business logic transformations
    4. **Marts Layer**: Final analytical models
    5. **Data Quality**: Comprehensive testing and validation
    """,
) as dag:
    
    # Start task
    start_pipeline = EmptyOperator(
        task_id="start_complete_pipeline",
        doc_md="Start of the complete dbt data pipeline"
    )
    
    # ========== STAGING TASKGROUP ==========
    with TaskGroup("staging_layer", tooltip="Process raw data into staging models") as staging_group:
        
        # Run staging models
        staging_models = DbtTaskGroup(
            group_id="run_staging_models",
            project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            render_config=RenderConfig(
                select=["path:models/staging"],
                enable_mock_profile=False,
                test_behavior=TestBehavior.AFTER_EACH,  # Run tests after each model
                env_vars={
                    "DBT_ENV": "staging",
                    "BATCH_DATE": "{{ ds }}",
                },
            ),
            execution_config=execution_config,
            profile_config=profile_config,
            operator_args={
                "install_deps": True,
                "full_refresh": False,
                "vars": {
                    "execution_date": "{{ ds }}",
                    "dag_run_id": "{{ dag_run.run_id }}",
                }
            },
            default_args={
                "retries": 1,
                "retry_delay": timedelta(minutes=3),
                "execution_timeout": timedelta(hours=1),
            },
        )
        
        # Test staging source data quality
        staging_source_tests = DbtTaskGroup(
            group_id="test_staging_sources",
            project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            render_config=RenderConfig(
                select=["source:*"],
                enable_mock_profile=False,
                test_behavior=TestBehavior.BUILD,
            ),
            execution_config=execution_config,
            profile_config=profile_config,
        )
        
        staging_source_tests >> staging_models
    
    # ========== SNAPSHOTS TASKGROUP ==========  
    with TaskGroup("snapshots_layer", tooltip="Create SCD Type 2 snapshots") as snapshots_group:
        
        # Run all snapshots
        run_snapshots = DbtTaskGroup(
            group_id="run_all_snapshots",
            project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            render_config=RenderConfig(
                select=["resource_type:snapshot"],
                enable_mock_profile=False,
                test_behavior=TestBehavior.AFTER_ALL,
                env_vars={
                    "DBT_ENV": "snapshots",
                    "BATCH_DATE": "{{ ds }}",
                },
            ),
            execution_config=execution_config,
            profile_config=profile_config,
            operator_args={
                "full_refresh": False,
                "vars": {
                    "execution_date": "{{ ds }}",
                    "snapshot_strategy": "check",
                }
            },
            default_args={
                "retries": 2,
                "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(hours=1),
            },
        )
        
        # Validate snapshot completeness
        validate_snapshots = EmptyOperator(
            task_id="validate_snapshot_completeness",
            doc_md="Placeholder for snapshot validation logic"
        )
        
        run_snapshots >> validate_snapshots
    
    # ========== INTERMEDIATE TASKGROUP ==========
    with TaskGroup("intermediate_layer", tooltip="Business logic transformations") as intermediate_group:
        
        # Sales intermediate models
        with TaskGroup("sales_models", tooltip="Sales-related intermediate models") as sales_subgroup:
            
            int_sales_models = DbtTaskGroup(
                group_id="run_int_sales",
                project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
                render_config=RenderConfig(
                    select=["path:models/intermediate", "tag:sales"],
                    enable_mock_profile=False,
                    test_behavior=TestBehavior.AFTER_EACH,
                ),
                execution_config=execution_config,
                profile_config=profile_config,
                default_args={
                    "retries": 1,
                    "execution_timeout": timedelta(minutes=45),
                },
            )
        
        # Customer intermediate models
        with TaskGroup("customer_models", tooltip="Customer-related intermediate models") as customer_subgroup:
            
            int_customer_models = DbtTaskGroup(
                group_id="run_int_customers",
                project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
                render_config=RenderConfig(
                    select=["path:models/intermediate", "tag:customer"],
                    enable_mock_profile=False,
                    test_behavior=TestBehavior.AFTER_EACH,
                ),
                execution_config=execution_config,
                profile_config=profile_config,
            )
        
        # Product intermediate models
        with TaskGroup("product_models", tooltip="Product-related intermediate models") as product_subgroup:
            
            int_product_models = DbtTaskGroup(
                group_id="run_int_products",
                project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
                render_config=RenderConfig(
                    select=["path:models/intermediate", "tag:product"],
                    enable_mock_profile=False,
                    test_behavior=TestBehavior.AFTER_EACH,
                ),
                execution_config=execution_config,
                profile_config=profile_config,
            )
        
        # All intermediate models can run in parallel
        [sales_subgroup, customer_subgroup, product_subgroup]
    
    # ========== MARTS TASKGROUP ==========
    with TaskGroup("marts_layer", tooltip="Final analytical models") as marts_group:
        
        # Core marts
        core_marts = DbtTaskGroup(
            group_id="run_core_marts",
            project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            render_config=RenderConfig(
                select=["path:models/marts/core"],
                enable_mock_profile=False,
                test_behavior=TestBehavior.AFTER_EACH,
                env_vars={
                    "DBT_ENV": "marts",
                    "BATCH_DATE": "{{ ds }}",
                },
            ),
            execution_config=execution_config,
            profile_config=profile_config,
            operator_args={
                "vars": {
                    "execution_date": "{{ ds }}",
                    "mart_refresh_mode": "incremental",
                }
            },
        )
        
        # Finance marts subgroup
        with TaskGroup("finance_marts", tooltip="Finance and analytics marts") as finance_subgroup:
            
            # Sales summary mart
            sales_summary_mart = DbtTaskGroup(
                group_id="run_sales_summary",
                project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
                render_config=RenderConfig(
                    select=["mart_sales_summary"],
                    enable_mock_profile=False,
                    test_behavior=TestBehavior.AFTER_EACH,
                ),
                execution_config=execution_config,
                profile_config=profile_config,
            )
            
            # Customer segment mart
            customer_segment_mart = DbtTaskGroup(
                group_id="run_customer_segments",
                project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
                render_config=RenderConfig(
                    select=["mart_customer_segment_run"],
                    enable_mock_profile=False,
                    test_behavior=TestBehavior.AFTER_EACH,
                ),
                execution_config=execution_config,
                profile_config=profile_config,
            )
            
            # Product performance mart
            product_performance_mart = DbtTaskGroup(
                group_id="run_product_performance",
                project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
                render_config=RenderConfig(
                    select=["mart_product_performance_run"],
                    enable_mock_profile=False,
                    test_behavior=TestBehavior.AFTER_EACH,
                ),
                execution_config=execution_config,
                profile_config=profile_config,
            )
            
            # Dependencies within finance marts
            [sales_summary_mart, customer_segment_mart] >> product_performance_mart
        
        # Dependencies within marts group
        core_marts >> finance_subgroup
    
    # ========== DATA QUALITY TASKGROUP ==========
    with TaskGroup("data_quality_checks", tooltip="Comprehensive data quality validation") as quality_group:
        
        # Run all remaining tests
        comprehensive_tests = DbtTaskGroup(
            group_id="run_comprehensive_tests",
            project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            render_config=RenderConfig(
                select=["test_type:generic", "test_type:singular"],
                enable_mock_profile=False,
                test_behavior=TestBehavior.BUILD,
            ),
            execution_config=execution_config,
            profile_config=profile_config,
            default_args={
                "retries": 0,  # Don't retry failed tests
                "execution_timeout": timedelta(minutes=30),
            },
        )
        
        # Business critical tests
        business_rule_tests = DbtTaskGroup(
            group_id="validate_business_rules",
            project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            render_config=RenderConfig(
                select=["tag:business_critical"],
                enable_mock_profile=False,  
                test_behavior=TestBehavior.BUILD,
            ),
            execution_config=execution_config,
            profile_config=profile_config,
        )
        
        # Generate documentation
        generate_docs = EmptyOperator(
            task_id="generate_documentation",
            doc_md="Generate dbt documentation and update data catalog"
        )
        
        comprehensive_tests >> business_rule_tests >> generate_docs
    
    # ========== NOTIFICATIONS TASKGROUP ==========
    with TaskGroup("notifications", tooltip="Pipeline completion notifications") as notification_group:
        
        send_success_notification = EmptyOperator(
            task_id="send_success_notification",
            doc_md="Send success notification to stakeholders"
        )
        
        update_data_catalog = EmptyOperator(
            task_id="update_data_catalog", 
            doc_md="Update enterprise data catalog with latest run metadata"
        )
        
        send_success_notification >> update_data_catalog
    
    # End task
    end_pipeline = EmptyOperator(
        task_id="end_complete_pipeline",
        doc_md="End of the complete dbt data pipeline"
    )
    
    # ========== MAIN PIPELINE DEPENDENCIES ==========
    start_pipeline >> staging_group >> snapshots_group >> intermediate_group >> marts_group >> quality_group >> notification_group >> end_pipeline