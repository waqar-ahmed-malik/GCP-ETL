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


with DAG('DAG_GCP_ERS_AUDIT', schedule_interval=None, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_ERS_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/audit/ers_audit_process.py --config "config.properties" --productconfig "ers.properties" --env {{var.value.environment}} --connectionprefix "d3"')
     
    
    t1