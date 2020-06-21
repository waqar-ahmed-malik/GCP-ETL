from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 21),
    'email': ['ramneek.kaur@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="00 09 * * *"

# pp = pprint.PrettyPrinter(indent=4)
# 
# def trigger_dag(context, dag_run_obj):
#     dag_run_obj.payload = {'message': context['params']['message']}
#     pp.pprint(dag_run_obj.payload)
#     return dag_run_obj

# POS_DATE = "'"+{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}+"'"
    

with DAG('DAG_GCP_POS_LOAD2', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_POS2_MART',
        bash_command="python /home/airflow/gcs/data/GCPDWH/connectsuite/load_pos_to_bigquery_landing_dataflow.py --config 'config.properties' --productconfig 'sam.properties' --env {{var.value.environment}} --connectionprefix 'cs' --incrementaldate '2' --inputdate {{ (execution_date - macros.timedelta(2)).strftime('%Y%m%d')  }} ")
    

    
#     t2 = TriggerDagRunOperator(
#         task_id='trigger_dagrun',
#         trigger_dag_id="DAG_GCP_OUTBOUND_POS_FILE_LOAD",
#         python_callable=trigger_dag,
#         params={'message': 'Triggering the DAG'},    
#         dag=dag,)       
     
    t1 
#     >> t2  
    