from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 7),
    'email': ['prerna.anand@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = "00 03 * * *"


with DAG('DAG_GCP_QUOTES_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    
     t1 = BashOperator(
          task_id='T1_COPY_TO_GCS_BUCKET',
          bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config config.properties --productconfig quotes.properties --env {{ var.value.environment }}'
          )

     t2 = BashOperator(
          task_id='T2_COPY_MWGQUOTES_FILES_TO_GCS_BUCKET',
          bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config config.properties --productconfig mwgquote.properties --env {{ var.value.environment }}'
          )

     t3 = BashOperator(
          task_id='T3_LOAD_MWG_QUOTES',
          bash_command='python /home/airflow/gcs/data/GCPDWH/quotes/mwg_quote_process.py --config config.properties --productconfig mwgquote.properties --env {{ var.value.environment }}'
           )
         
     t4 = BashOperator(
          task_id='T4_LOAD_QUOTES',
          bash_command='python /home/airflow/gcs/data/GCPDWH/quotes/process_quotes.py --config config.properties --productconfig quotes.properties --env {{ var.value.environment }}'
           )
               
     t1 >> t2 >> t3 >> t4
