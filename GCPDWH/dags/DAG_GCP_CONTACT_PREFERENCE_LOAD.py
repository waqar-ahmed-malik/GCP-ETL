from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o3, 0o1),
    'email': ['kishore.boddu@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = '00 12 * * *'

with DAG('DAG_GCP_CONTACT_PREFERENCE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/contact_preference/') as dag:
   
    t1 = BigQueryOperator(
        task_id='bq_task_create_contact_preferences',
        bql='/work_contact_preferences.sql',
        destination_dataset_table='LANDING.WORK_CONTACT_PREFERENCES',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t2 = BigQueryOperator(
        task_id='bq_task_load_customer_contact_preference_dim',
        bql='/sam_contact_preferences.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    
t1 >>t2
