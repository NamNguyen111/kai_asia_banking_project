# import sys
# sys.path.append('/opt/airflow/utils')

# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.operators.python import PythonOperator

# # Import trực tiếp file, không qua package
# import connect_db_from_airflow

# default_args = {
#     'description': 'A DAG to test connect to db',
#     'start_date': datetime(2025, 7, 14),
#     'catchup': False, 
# }

# dag = DAG(
#     dag_id='test_connect',
#     default_args=default_args,
#     schedule=timedelta(minutes=10)
# )

# def main():
#     try:
#         # Gọi function từ module connect_db
#         conn = connect_db_from_airflow.connect_to_db()
#         print("Ket noi ok")
#     except Exception as e:
#         print(f"There's an exception {e}")
#     finally:
#         if 'conn' in locals():
#             conn.close()
#             print("Database connection closed.")

# with dag:
#     task1 = PythonOperator(
#         task_id='connect_db_test',
#         python_callable=main
#     )
#     task1