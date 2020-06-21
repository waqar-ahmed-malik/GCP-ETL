from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['ramneek.kaur@norcal.aaa.com'],
    'email_on_failure':True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="00 03 * * *"

with DAG('DAG_GCP_GLOBALWARE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    t0 = DummyOperator(task_id='dummy')
    t1 = BashOperator(
        task_id='T1_LOAD_GLOBALWARE_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/globalware/load_globalware_to_bigquery_dataflow.py --config "config.properties" --productconfig "globalware.properties" --env {{var.value.environment}} --connectionprefix "gw" --inputstartdate {{ (execution_date - macros.timedelta(1)).strftime("%Y-%m-%d")  }} --inputenddate {{ execution_date.strftime("%Y-%m-%d") }}')
    
    t2 = BashOperator(
        task_id='T1_LOAD_GLOBALWARE_TRUNCATE_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/globalware/load_globalware_truncate_tables.py --config "config.properties" --productconfig "globalware.properties" --env {{var.value.environment}} --connectionprefix "gw" ')
    
    
    t0 >> [t1,t2]