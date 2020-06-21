from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o3, 0o1),
    'email': ['aneel.akkena@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = "30 11 * * *"
# Temp schedule interval for testing
# schedule_interval = timedelta(hours=24)

with DAG('DAG_GCP_OUTBOUND_DRIVER_IDENTIFICATION_FILE', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/smart_driver') as dag:
    t1 = BigQueryOperator(
        task_id='bq_task_stg_driver_indentification',
        bql='/outbound_stage_driver_indentification.sql',
        destination_dataset_table='LANDING.OUTBOUND_SMART_DRIVER_IDENTIFICATION',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
              
    t2 = BigQueryToCloudStorageOperator(
        task_id='bq_task_outbound_smart_driver_file',
        source_project_dataset_table='LANDING.OUTBOUND_SMART_DRIVER_IDENTIFICATION',
        destination_cloud_storage_uris=['gs://{{var.value.smart_driver_outbound}}/driver_indentification_{{ds_nodash}}.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',',
        print_header=False)         
          
t1 >>t2
