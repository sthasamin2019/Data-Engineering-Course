from airflow.sdk import DAG
from airflow.sdk import TaskGroup

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def python_transform(**context):
    row_extracted =  context["ti"].xcom_pull(task_ids = "extract",key="row_count")
    print(row_extracted)
    print("hello! Transform from python")

def python_transform2(**context):
    pass

def python_extract(**context):
    row_extracted = 4500
    context["ti"].xcom_push(key = "row_count", value = row_extracted)
    print("extracting data from file")

def python_load(**context):
    print("Loading data to DB")
    
def notify_success(**context):
    print("pipeline finished - sending message to slack")

def alert_on_failure(**context):
    print("pipeline failed")

with DAG(
    description= "This is my first DAG",
    dag_id='operator_xcom',
    schedule='0 9-15 * * 1-5'
) as dag:
    extract = PythonOperator(task_id = 'extract',python_callable=python_extract)

    with TaskGroup(group_id='transform') as transform:
        transform1 = PythonOperator(task_id = 'transform1',python_callable=python_transform, retries=3,retry_delay=timedelta(seconds=5))
        transform2 = PythonOperator(task_id = 'transform2',python_callable=python_transform2, retries=3,retry_delay=timedelta(seconds=5))

    load = PythonOperator(task_id = 'load',python_callable=python_load)

    extract >> transform >> load
