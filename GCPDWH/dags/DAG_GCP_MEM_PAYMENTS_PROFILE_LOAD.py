from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o3, 0o1),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}


with DAG('DAG_GCP_MEM_PAYMENTS_PROFILE_LOAD', schedule_interval=None, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_LOAD_MEMBERSHIP_PAYMENTS_PROFILE_MART',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_mem_payment_profile_to_bigquery_landing_dataflow.py --config "config.properties" --productconfig "connectsuite.properties" --env {{var.value.environment}} --connectionprefix "cs" --inputdate "{{ execution_date.strftime("%Y-%m-%d") }}"')
    
 
    t1