from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os
import time


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 23),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = "00 06 * * *"

with DAG('DAG_GCP_AMEX_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    

     t1 = BashOperator(
          task_id='T1_LOAD_AMEX',
          bash_command='python /home/airflow/gcs/data/GCPDWH/amex/process_american_express.py --config config.properties --productconfig amex.properties --env {{var.value.environment}} --inputdate {{ (execution_date - macros.timedelta(days=1)).strftime("%y%m%d")  }}'
           )
     
     t1  