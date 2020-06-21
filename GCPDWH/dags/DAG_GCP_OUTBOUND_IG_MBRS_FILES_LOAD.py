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
    'gsutil cat gs://{{var.value.ig_outbound_zone}}/NCNU_MEM_Audit.tmp | sed \'s/"//g\' > /tmp/NCNU_MEM_Audit{{ execution_date.strftime("%Y%m%d%H%M%S") }}005.dat; '
    'gsutil mv /tmp/NCNU_MEM_Audit{{ execution_date.strftime("%Y%m%d%H%M%S") }}005.dat gs://{{var.value.ig_outbound_zone}}/NCNU_MEM_Audit{{ execution_date.strftime("%Y%m%d%H%M%S") }}005.dat; '
    'gsutil rm -f gs://{{var.value.ig_outbound_zone}}/NCNU_MEM_Audit.tmp')


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

schedule_interval = "00 10 * * *"
# Temp schedule interval for testing
# schedule_interval = timedelta(hours=24)

with DAG('DAG_GCP_OUTBOUND_IG_MBRS_FILE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/membership_ig') as dag:
    
    t1 = BigQueryOperator(
        task_id='bq_task_stg_ig_mbrs',
        bql='/outbound_mbrship_ig.sql',
        destination_dataset_table='LANDING.OUTBOUND_STAGE_MBRS_IG',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t2 = BigQueryOperator(
        task_id='bq_task_stg_ig_mkt_mbrs',
        bql='/outbound_mbrship_ig_mkt.sql',
        destination_dataset_table='LANDING.OUTBOUND_STAGE_IG_MARKETING',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t3 = BigQueryOperator(
        task_id='bq_task_stg_ig_mbrs_audit',
        bql='/outbound_mbrship_ig_audit.sql',
        destination_dataset_table='LANDING.OUTBOUND_STAGE_MBRS_IG_AUDIT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
            
    t4 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_ig_mbrs_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_STAGE_MBRS_IG',
        destination_cloud_storage_uris=['gs://{{var.value.ig_outbound_zone}}/mbrfile{{ execution_date.strftime("%Y%m%d%H%M%S") }}005.dat'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
    
    t5 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_ig_marketing_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_STAGE_IG_MARKETING',
        destination_cloud_storage_uris=['gs://{{var.value.ig_outbound_zone}}/NCNU_MEM_MKT_{{ execution_date.strftime("%Y%m%d%H%M%S") }}005.dat'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
  
    t6 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_ig_mbrs_audit_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_STAGE_MBRS_IG_AUDIT',
        destination_cloud_storage_uris=['gs://{{var.value.ig_outbound_zone}}/NCNU_MEM_Audit.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
    
    t7 = BashOperator(
        task_id='reformat_file_to_auditfile',
        bash_command=bash_cmd)

t1 >>t2 >>t3 >>t4>>t5>>t6>>t7
