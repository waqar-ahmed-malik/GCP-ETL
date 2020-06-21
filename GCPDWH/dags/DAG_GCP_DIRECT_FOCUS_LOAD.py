from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o4, 24),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="00 18 * * *"

with DAG('DAG_GCP_DIRECT_FOCUS_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_DIRECT_FOCUS_COMMUNICATION',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_direct_focus_to_bigquery_landing_dataflow.py --config "config.properties" --productconfig "directfocus.properties" --env "{{var.value.environment}}" --connectionprefix "df" --incrementaldate 1 --inputdate {{ execution_date.strftime("%Y-%m-%d") }}')
    
 
    t1    
