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
    'gsutil cat gs://{{var.value.ar_decline_outbound_zone}}/ar_decline_payment_file.tmp | sed \'s/"""""//g\' | sed \'s/""/"/g\' > /tmp/{{ execution_date.strftime("%Y%m%d") }}_{{ execution_date.strftime("%H%M%S") }}_Aro_Payment_Decline.txt; '
    'gsutil cat gs://{{var.value.ar_decline_outbound_zone}}/ar_soft_decline_payment_file.tmp | sed \'s/"""""//g\' | sed \'s/""/"/g\' > /tmp/{{ execution_date.strftime("%Y%m%d") }}_{{ execution_date.strftime("%H%M%S") }}_Aro_Payment_Soft_Decline.txt; '
    'gsutil mv /tmp/{{ execution_date.strftime("%Y%m%d") }}_{{ execution_date.strftime("%H%M%S") }}_Aro_Payment_Decline.txt gs://{{var.value.ar_decline_outbound_zone}}/{{ execution_date.strftime("%Y%m%d") }}_{{ execution_date.strftime("%H%M%S") }}_Aro_Payment_Decline.txt; '
    'gsutil mv /tmp/{{ execution_date.strftime("%Y%m%d") }}_{{ execution_date.strftime("%H%M%S") }}_Aro_Payment_Soft_Decline.txt gs://{{var.value.ar_decline_outbound_zone}}/{{ execution_date.strftime("%Y%m%d") }}_{{ execution_date.strftime("%H%M%S") }}_Aro_Payment_Soft_Decline.txt; '
    'gsutil rm -f gs://{{var.value.ar_decline_outbound_zone}}/ar_decline_payment_file.tmp;'     
    'gsutil rm -f gs://{{var.value.ar_decline_outbound_zone}}/ar_soft_decline_payment_file.tmp;'
    )


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

#schedule_interval = '00 15 * * 1'
# Temp schedule interval for testing
#schedule_interval = timedelta(hours=24)

with DAG('DAG_GCP_OUTBOUND_AR_DECLINE_FILE_LOAD', schedule_interval=None, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/ar_payment_decline') as dag:
    t1 = BigQueryOperator(
        task_id='bq_task_stg_ar_hard_decline',
        bql='/stage_aro_payment_decline.sql',
        destination_dataset_table='OPERATIONAL.MEMBERSHIP_AR_DECLINED_PAYMENTS',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t2 = BigQueryOperator(
        task_id='bq_task_create_ar_hard_outbound',
        bql='/outbound_aro_payment_decline.sql',
        destination_dataset_table='LANDING.OUTBOUND_AR_HARD_DECLINE_PAYMENT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
   
    t3 = BigQueryOperator(
        task_id='bq_task_stg_ar_soft_decline',
        bql='/stage_aro_soft_decline_payment.sql',
        destination_dataset_table='OPERATIONAL.MEMBERSHIP_AR_DECLINED_PAYMENTS',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t4 = BigQueryOperator(
        task_id='bq_task_create_ar_soft_outbound',
        bql='/outbound_aro_soft_decline_payment.sql',
        destination_dataset_table='LANDING.OUTBOUND_AR_SOFT_DECLINE_PAYMENT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t5 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_ar_payment_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_AR_HARD_DECLINE_PAYMENT',
        destination_cloud_storage_uris=['gs://{{var.value.ar_decline_outbound_zone}}/ar_decline_payment_file.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)    
    
    t6 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_ar_soft_decline_payment_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_AR_SOFT_DECLINE_PAYMENT',
        destination_cloud_storage_uris=['gs://{{var.value.ar_decline_outbound_zone}}/ar_soft_decline_payment_file.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
    
    t7 = BashOperator(
        task_id='reformat_file_to_fixed_width',
        bash_command=bash_cmd)
                  

t1 >>t2 >>t3 >>t4 >>t5 >>t6 >>t7
