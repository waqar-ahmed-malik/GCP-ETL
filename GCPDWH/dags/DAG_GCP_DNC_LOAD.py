from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
                              
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 31),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
    }


with DAG('DAG_GCP_DNC_LOAD', schedule_interval=None, catchup=False, default_args=default_args) as dag:
      
      t1 = BashOperator(
        task_id='T1_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/dnc/process_dnc.py --config "config.properties" --productconfig "dnc.properties" --env "{{var.value.environment}}" --inputdate "{{ execution_date.strftime("%m%d%y") }}"' )

t1