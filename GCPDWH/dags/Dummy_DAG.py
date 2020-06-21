from airflow import DAG
from datetime import datetime, timedelta
import time
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 16),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],#['dlitgcpdwsupport@norcal.aaa.com']
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}


schedule_interval = "30 12 * * *"

with DAG('Dummy_DAG', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

     t1 = BashOperator(
          task_id='T1_COPY_TO_GCS_BUCKET'
          )
     
     t1  
