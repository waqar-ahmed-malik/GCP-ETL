from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 16),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = "30 12 * * *"

with DAG('DAG_GCP_DISCOVER_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    

     t1 = BashOperator(
          task_id='T1_LOAD_DISCOVER',
          bash_command='python /home/airflow/gcs/data/GCPDWH/discover/process_discover.py --config config.properties --productconfig discover.properties --env {{var.value.environment}}'
           )
     
     t1  
     
     