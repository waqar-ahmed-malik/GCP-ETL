from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
                              
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 10),
    'email': ['prerna.anand@norcal.aaa.com'],#['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
    }

schedule_interval='00 12 * * *'

with DAG('DAG_GCP_EXTERNAL_PAYMENTS_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
      t1 = BashOperator(
        task_id='T1_GCP_MOVE',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "ext_payments.properties" --env {{ var.value.environment }}')
      
      t2 = BashOperator(
        task_id='T2_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/extpayments/process_ext_payments.py --config "config.properties" --productconfig "ext_payments.properties" --env {{ var.value.environment }}' )
        

t1 >> t2