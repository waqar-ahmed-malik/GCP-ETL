from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 21),
    'email': ['dlitgcpdwsupport@norcal.aaa.com','ramneek.kaur@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="00 08 * * *"

with DAG('DAG_GCP_SAM_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_SAM_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_sam_to_bigquery_landing_dataflow.py --config "config.properties" --productconfig "sam.properties" --env {{var.value.environment}} --connectionprefix "cs" --incrementaldate 5 --inputdate {{ execution_date.strftime("%Y-%m-%d") }}')
    
 
t1   
    