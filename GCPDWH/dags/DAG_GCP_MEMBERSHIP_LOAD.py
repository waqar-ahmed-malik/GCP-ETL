from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import time
import pprint

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="30 08 * * *"

pp = pprint.PrettyPrinter(indent=4)

def trigger_dag(context, dag_run_obj):
    dag_run_obj.payload = {'message': context['params']['message']}
    pp.pprint(dag_run_obj.payload)
    return dag_run_obj

with DAG('DAG_GCP_MEMBERSHIP_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_MEMBERSHIP_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_mem_to_bigquery_landing_dataflow.py --config "config.properties" --productconfig "connectsuite.properties" --env {{var.value.environment}} --connectionprefix "cs" --incrementaldate 1 --inputdate {{ execution_date.strftime("%Y-%m-%d") }}')
   
    t2 = TriggerDagRunOperator(
            task_id='TRIGGER_NOTIFICATION_XML_DAG',
            trigger_dag_id="DAG_OUTBOUND_MEMBERSHIP_NOTIFICATION_XML",
            python_callable=trigger_dag,
            params={'message': 'Triggering the DAG'},    
            dag=dag,)       
    t3 = TriggerDagRunOperator(
            task_id='TRIGGER_AUDIT_DAG',
            trigger_dag_id="DAG_GCP_CONNECTSUITE_AUDIT",
            python_callable=trigger_dag,
            params={'message': 'Triggering the DAG FOR AUDIT'},    
            dag=dag,)
     
    t1 >> t2 >> t3
