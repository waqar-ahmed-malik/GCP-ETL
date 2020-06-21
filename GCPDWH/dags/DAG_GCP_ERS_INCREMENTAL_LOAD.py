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
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval="00 10 * * *"

pp = pprint.PrettyPrinter(indent=4)
def trigger_dag(context, dag_run_obj):
    dag_run_obj.payload = {'message': context['params']['message']}
    pp.pprint(dag_run_obj.payload)
    return dag_run_obj

with DAG('DAG_GCP_ERS_INCREMENTAL_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    
    t1 = BashOperator(
        task_id='T1_INCREMENT_PARAM_FILES',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ers/outbound_ers_incr_param_files.py --config "config.properties" --productconfig "ers.properties" --env {{var.value.environment}}')
    
    t2 = BashOperator(
        task_id='T2_TRUNCATE_WORK_TABLES',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ers/truncate_ers_work_tables.py --config "config.properties" --productconfig "ers.properties" --env {{var.value.environment}} --sqlfile "sql/incremental/delete_incr_work_tables.sql" ')
    
    t3 = BashOperator(
        task_id='T3_LOAD_ERS_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ers/load_ers_incr_tables_to_bq_landing_dataflow.py --config "config.properties" --productconfig "ers.properties" --env {{var.value.environment}} --connectionprefix "d3" --incrementaldate 1 ')
    
    t4 = TriggerDagRunOperator(
            task_id='T4_TRIGGER_ERS_AUDIT_DAG',
            trigger_dag_id="DAG_GCP_ERS_AUDIT",
            python_callable=trigger_dag,
            params={'message': 'Triggering the DAG'},    
            dag=dag,)       
    
    t1 >> t2 >> t3 >> t4