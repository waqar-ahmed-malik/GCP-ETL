from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import time
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 7),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}




schedule_interval = "00 03 * * TUE-SUN"

with DAG('DAG_GCP_AS400_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
     
	 
     t1 = BashOperator(
     task_id='T1_GCP_AS400_SEGMENTS_LOAD',
     bash_command='python /home/airflow/gcs/data/GCPDWH/ivans/load_as400_segments_to_bq_dataflow.py --config config.properties --productconfig ivans.properties --env {{var.value.environment}} --separator "|" --stripheader 0 --stripdelim 0 --addaudit 1 --writeDeposition WRITE_APPEND --system AS400 --input gs://dw-prod-ivans/current/AS400_NCNU_{{ (execution_date - macros.timedelta(1)).strftime("%Y%m%d")  }}.DAT')
     
     
     t1
  