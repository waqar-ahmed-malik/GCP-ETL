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


schedule_interval = "00 03 * * *"

with DAG('DAG_GCP_CLAIMS_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

     t1 = BashOperator(
          task_id='T1_COPY_TO_GCS_BUCKET',
          bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config config.properties --productconfig claims.properties --env {{var.value.environment}}'
          )
     t2 = BashOperator(
          task_id='T2_LOAD_CLAIMS_PROCESSING',
          bash_command='python /home/airflow/gcs/data/GCPDWH/claims/process_claims_load.py --config config.properties --productconfig claims.properties --env {{var.value.environment}} --input current/CLAIMS_NCNU_{{ (execution_date - macros.timedelta(1)).strftime("%Y%m%d")  }}'
           )
     
     t1 >> t2 