from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 19),
    'email': ['prerna.anand@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = "07 45 * * *"
#now = datetime.datetime.now().strftime ("%m.%d.%Y")

with DAG('DAG_GCP_MSI_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    
     t1 = BashOperator(
          task_id='T1_COPY_TO_GCS_BUCKET',
          bash_command='python /home/airflow/gcs/data/GCPDWH/msi/transfer_mountpoint_to_gcs_msi.py --config config.properties --productconfig msi.properties --env prod'
          )
         
     t2 = BashOperator(
          task_id='T2_LOAD_MSI',
          bash_command='python /home/airflow/gcs/data/GCPDWH/msi/process_msi.py --config config.properties --productconfig msi.properties --env prod'
           )
     
     t1 >> t2 