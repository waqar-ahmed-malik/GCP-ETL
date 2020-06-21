from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 12),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = '30 18 1-8,10-31 * *'


with DAG('DAG_GCP_DIRECTFOCUS_INDIVIDUAL_DAILY_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    
     t1 = BashOperator(
          task_id='T1_LOAD_DIRECTFOCUS_INDIVIDUAL',
          bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_individual_to_bigquery_dataflow.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1'
           )
               
     t1 
