from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 19),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = '00 19 * * MON-SAT'


with DAG('DAG_GCP_DIRECT_FOCUS_ENGAGE_JOURNEY_HISTORY_WEEKDAY_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    
     t1 = BashOperator(
          task_id='T1_LOAD_DIRECTFOCUS_ENGAGE_JOURNEY_HISTORY',
          bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/process_engage_journey_history.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1'
           )
               
     t1 
