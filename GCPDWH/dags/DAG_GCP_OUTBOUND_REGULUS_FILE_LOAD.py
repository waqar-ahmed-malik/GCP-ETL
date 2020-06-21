from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
import logging

bash_cmd = (
    'gcloud auth activate-service-account {{var.value.service_account_email}} --key-file={{var.value.key_file}}; '
    'gcloud config set project="{{var.value.project}}"; '
    'gsutil cat gs://{{var.value.regulus_outbound_zone}}/regulus_file.tmp | sed \'s/""/"/g\' > /tmp/aaalookup_regulus_{{ds_nodash}}.dat; '
    'gsutil mv /tmp/aaalookup_regulus_{{ds_nodash}}.dat gs://{{var.value.regulus_outbound_zone}}/aaalookup_regulus_{{ds_nodash}}.dat; '
    'gsutil rm -f gs://{{var.value.regulus_outbound_zone}}/regulus_file.tmp')

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

schedule_interval = "00 09 * * *"
# Temp schedule interval for testing
# schedule_interval = timedelta(hours=24)

with DAG('DAG_GCP_OUTBOUND_REGULUS_FILE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/regulus') as dag:
    
    t1 = BigQueryOperator(
        task_id='bq_task_stg_regulus',
        bql='/outbound_stage_regulus.sql',
        destination_dataset_table='LANDING.OUTBOUND_STAGE_REGULUS',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t2 = BigQueryOperator(
        task_id='bq_task_stg_regulus_audit',
        bql='/outbound_stage_regulus_audit.sql',
        destination_dataset_table='LANDING.OUTBOUND_STAGE_REGULUS_AUDIT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
            
    t3 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_regulus_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_STAGE_REGULUS',
        destination_cloud_storage_uris=['gs://{{var.value.regulus_outbound_zone}}/regulus_file.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
  
    t4 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_regulus_audit_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_STAGE_REGULUS_AUDIT',
        destination_cloud_storage_uris=['gs://{{var.value.regulus_outbound_zone}}/aaalookup_regulus_audit_{{ds_nodash}}.dat'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
      
    t5 = BashOperator(
        task_id='reformat_file_to_fixed_width',
        bash_command=bash_cmd)

          
t1 >>t2 >>t3 >>t4>>t5
