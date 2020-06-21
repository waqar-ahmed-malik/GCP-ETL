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
    'email': ['kishore.boddu@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = "00 10 15 * *"
# Temp schedule interval for testing
# schedule_interval = timedelta(hours=24)

with DAG('DAG_GCP_OUTBOUND_PAYMODE_FILE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/paymode') as dag:
    t1 = BigQueryOperator(
        task_id='bq_task_stg_paymode',
        bql='/outbound_stage_paymode.sql',
        destination_dataset_table='LANDING.OUTBOUND_STAGE_PAYMODE',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
              
    t2 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_paymode_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_STAGE_PAYMODE',
        destination_cloud_storage_uris=['gs://{{var.value.paymode_outbound_zone}}/aaalookup{{ds_nodash}}.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
  
         
          
t1 >>t2
