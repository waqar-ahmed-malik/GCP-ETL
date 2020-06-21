from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from datetime import datetime, timedelta

GCBHook = BigQueryHook(bigquery_conn_id='aaa_bq_conn')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 17),
    'email':['prerna.anand@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
    }

schedule_interval='00 16 1,15 * *'


with  DAG('DAG_GCP_PIPELINE_CALENDAR', default_args=default_args,catchup=False, schedule_interval=schedule_interval) as dag:

 t1 = BigQueryOperator(
     task_id='bigquery_test',
     bql='update LANDING.PIPELINE_CALENDAR set FLAG=True where start_date=(SELECT min(start_Date) FROM LANDING.PIPELINE_CALENDAR where flag=False) and end_Date=(SELECT min(end_Date) FROM LANDING.PIPELINE_CALENDAR where flag=False)',
     destination_dataset_table=False,
     bigquery_conn_id='aaa_bq_conn',
     delegate_to=False,
     udf_config=False,
     use_legacy_sql=False,
     dag=dag,
)

t1