from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['ramneek.kaur@norcal.aaa.com'],
    'email_on_failure':True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="00 11 * * *"

with DAG('DAG_GCP_COR_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_COR_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/protractor/process_protractor.py --config "config.properties" --productconfig "protractor.properties" --env {{var.value.environment}} ')
    
    
    t1