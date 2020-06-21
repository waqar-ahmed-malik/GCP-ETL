from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 11),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=360)
}


schedule_interval = "00 13 * * *"


with DAG('DAG_GCP_TRAVEL_TST_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
#      t1 = BashOperator(
#      task_id='T1_COPY_TO_GCS',
#      bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "travelware.properties" --env "{{var.value.environment}}"'
#      )

     t2 = BashOperator(
        task_id='T2_TST_BIGQUERY_LANDING',
        bash_command='python /home/airflow/gcs/data/GCPDWH/travelware/load_travel_tst_to_bigquery_landing.py --config config.properties --productconfig travelware.properties --env {{var.value.environment}} --input gs://dw-prod-inbound/travel-tst/current/AAANCNU_Data_{{ (execution_date - macros.timedelta(1)).strftime("%Y%m%d")  }}.psv --separator "|" --stripheader 1 --stripdelim 0 --addaudit 1 --output TRAVEL_TST_LDG --writeDeposition WRITE_APPEND')
     
     t3 = BashOperator(
        task_id= 'TRAVEL_TRANSACTION_FACT',
        bash_command='python /home/airflow/gcs/data/GCPDWH/travelware/load_travel_transaction_fact_dataflow.py --config config.properties --productconfig travelware.properties --env {{var.value.environment}} ')

     t4 = BashOperator(
        task_id='T4_LOAD_MDM_TRAVEL',
        bash_command='python /home/airflow/gcs/data/GCPDWH/travelware/mdm_travel.py --config "config.properties" --productconfig "travelware.properties" --env {{var.value.environment}}')
     
     
     t2 >> t3 >> t4
#      t1 >> t2 >>t3
   