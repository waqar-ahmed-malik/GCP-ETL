from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import time
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 19),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

def trigger_insurance_mart(context, dag_run_obj):
    dag_run_obj.payload = {
        "message": context["dag_run"].conf["message"],
        "day": context["dag_run"].conf["day"]
    }
    return dag_run_obj

batchdate =  (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
print(batchdate)

filename_IE = "gs://dw-prod-ivans/current/IE_NCNU_" + batchdate + ".DAT"

schedule_interval = "30 19 * * *"

with DAG('DAG_GCP_IE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
     
     t1 = BashOperator(
     task_id='T1_COPY_TO_GCS',
     bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "ivans.properties" --env "prod"'
     )
     
     t2 = BashOperator(
        task_id='T2_GCP_IE_SEGMENTS_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ivans/load_ie_segments_to_bq_dataflow.py --config config.properties --productconfig ivans.properties --env prod --separator "|" --stripheader 0 --stripdelim 0 --addaudit 1 --writeDeposition WRITE_APPEND --system IE --input ' + filename_IE)
  
    
     
     t1 >> t2
   