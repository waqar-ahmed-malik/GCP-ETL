from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o6, 26),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="30 11 * * *"

with DAG('DAG_GCP_MDM_REMERGING', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_GCP_MDM_REMERGING',
        bash_command='python /home/airflow/gcs/data/GCPDWH/mdm/mdm_address_remerge_processing.py --config "config.properties" --productconfig "commlog.properties" --env {{var.value.environment}}')
      
    t1