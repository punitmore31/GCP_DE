# from airflow.models import DAG
# from airflow.operators.python import PythonOperator  # Corrected import
# from datetime import datetime, timedelta          # Corrected import (timedelta)

# def print_hello():
#     return 'Hello World!'

# default_args = {
#     'owner': 'airflow',
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),  # Corrected argument (timedelta)
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'catchup': False,
# }

# with DAG(
#     dag_id='hello_world',  
#     default_args=default_args,
#     start_date : datetime(2025, 12, 11),
#     schedule_interval='@daily',
#     tags=['example']
# ) as dag:

#     # This block must be indented
#     hello_task = PythonOperator(
#         task_id='hello_task',
#         python_callable=print_hello
#     )



