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

schedule_interval = '00 10 10 * *'
# Temp schedule interval for testing
# schedule_interval = timedelta(hours=24)

with DAG('DAG_GCP_OUTBOUND_NATIONAL_BUNDLE_FILE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/national-bundle') as dag:
   
    t1 = BigQueryOperator(
        task_id='bq_task_create_national_bundle_outbound',
        bql='/outbound_national_bundle_extract.sql',
        destination_dataset_table='LANDING.OUTBOUND_NATIONAL_BUNDLE_EXTRACT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t2 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_ntaional_bundle_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_NATIONAL_BUNDLE_EXTRACT',
        destination_cloud_storage_uris=['gs://{{var.value.national_bundle_outbound_zone}}/bundle_{{ execution_date.strftime("%Y%m%d")}}.txt.zip'],
        export_format='CSV',
        compression='GZIP',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
    
          
t1 >>t2

