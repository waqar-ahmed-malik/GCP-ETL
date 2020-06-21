from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['atul.guleria@norcal.aaa.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

# schedule_interval="30 08 * * *"

with DAG('TEST_DAG_GCP_ERS_LOAD', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    
    t1 = BashOperator(
        task_id='T1_INCREMENT_PARAM_FILES',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ers/test_outbound_ers_param_files.py   --config "config.properties" --productconfig "ers.properties" --env "{{var.value.environment}}"')
    t2 = BashOperator(
        task_id='T2_TRUNCATE_WORK_TABLES',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ers/test_truncate_ers_work_tables.py --config "config.properties" --productconfig "ers.properties" --env "{{var.value.environment}}" --sqlfile "test_delete_work_tables.sql" ')
    t3 = BashOperator(
        task_id='T3_LOAD_ERS_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ers/test_load_ers_to_bigquery_landing_dataflow.py --config "config.properties" --productconfig "ers.properties" --env "{{var.value.environment}}" --connectionprefix "d3" --incrementaldate 1 --inputdate {{ (execution_date - macros.timedelta(1)).strftime("%Y%m%d")  }}')
   
    
    t1 >> t2 >> t3 