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

schedule_interval="00 16 * * 3"

with DAG('DAG_GCP_COMM_LOG_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_COMM_LOG',
        bash_command='python /home/airflow/gcs/data/GCPDWH/comm_log/process_comm_log.py --config "config.properties" --productconfig "commlog.properties" --env {{var.value.environment}}')
     
#     t2 = BashOperator(
#         task_id='T2_LOAD_MDM_COMM_LOG',
#         bash_command='python /home/airflow/gcs/data/GCPDWH/comm_log/mdm_comm_logb.py --config "config.properties" --productconfig "commlog.properties" --env {{var.value.environment}}')
#      
        
    t1