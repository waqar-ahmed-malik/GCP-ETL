from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 10),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = "00 14 * * *"

# bashi = '{{"/home/airflow/gcs/data/GCPDWH/shell_scripts/copy_datasets.sh"}}'
with DAG('DAG_GCP_COPY_DATASETS', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_COPY_DATASETS',
        bash_command='python /home/airflow/gcs/data/GCPDWH/shell_scripts/process_shell.py --config config.properties --env prod')
    

t1 