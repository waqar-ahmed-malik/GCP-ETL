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
    'gsutil cat gs://{{var.value.revrec_outbound_zone}}/CS_RR2_FEES_Workday.tmp | sed \'s/"//g\' > /tmp/CS_RR2_FEES_Workday.txt; '
    'gsutil mv /tmp/CS_RR2_FEES_Workday.txt gs://{{var.value.revrec_outbound_zone}}/CS_RR2_FEES_Workday.txt; '
    'gsutil cat gs://{{var.value.revrec_outbound_zone}}/CS_RR2_REFUNDS_Workday.tmp | sed \'s/"//g\' > /tmp/CS_RR2_REFUNDS_Workday.txt; '
    'gsutil mv /tmp/CS_RR2_REFUNDS_Workday.txt gs://{{var.value.revrec_outbound_zone}}/CS_RR2_REFUNDS_Workday.txt; '
    'gsutil cat gs://{{var.value.revrec_outbound_zone}}/CS_RR2_VOID_Workday.tmp | sed \'s/"//g\' > /tmp/CS_RR2_VOID_Workday.txt; '
    'gsutil mv /tmp/CS_RR2_VOID_Workday.txt gs://{{var.value.revrec_outbound_zone}}/CS_RR2_VOID_Workday.txt; '
    'gsutil cat gs://{{var.value.revrec_outbound_zone}}/CS_RR2_DUES_Workday.tmp | sed \'s/"//g\' > /tmp/CS_RR2_DUES_Workday.txt; '
    'gsutil mv /tmp/CS_RR2_DUES_Workday.txt gs://{{var.value.revrec_outbound_zone}}/CS_RR2_DUES_Workday.txt; '	
    'gsutil rm -f gs://{{var.value.revrec_outbound_zone}}/CS_RR2_FEES_Workday.tmp;'
    'gsutil rm -f gs://{{var.value.revrec_outbound_zone}}/CS_RR2_REFUNDS_Workday.tmp;'
    'gsutil rm -f gs://{{var.value.revrec_outbound_zone}}/CS_RR2_VOID_Workday.tmp;'
    'gsutil rm -f gs://{{var.value.revrec_outbound_zone}}/CS_RR2_DUES_Workday.tmp;'	
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

schedule_interval = '00 15 * * *'
# Temp schedule interval for testing
# schedule_interval = timedelta(hours=24)

with DAG('DAG_GCP_OUTBOUND_REVREC_FILE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/membership_revrec') as dag:
    t1 = BigQueryOperator(
        task_id='bq_task_stg_rr_membership_payment',
        bql='/work_rr_membership_payment.sql',
        destination_dataset_table='LANDING.WORK_REVREC_MEMBERSHIP_PAYMENT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t2 = BigQueryOperator(
        task_id='bq_task_stg_rr_membership_refund',
        bql='/work_rr_membership_refund.sql',
        destination_dataset_table='LANDING.WORK_REVREC_MEMBERSHIP_REFUND',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
		
    t3 = BigQueryOperator(
        task_id='bq_task_merge_rr_membership_payment',
        bql='/merge_rr_membership_payment.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
		
    t4 = BigQueryOperator(
        task_id='bq_task_merge_rr_membership_refund',
        bql='/merge_rr_membership_refund.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
		
    t5 = BigQueryOperator(
        task_id='bq_task_outbound_revrec_fees_workday',
        bql='/outbound_revrec_fees_workday.sql',
        destination_dataset_table='LANDING.OUTBOUND_REVREC_PAYMENT_FEES_WORKDAY',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t6 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_revrec_fees_workday_file',
        source_project_dataset_table='LANDING.OUTBOUND_REVREC_PAYMENT_FEES_WORKDAY',
        destination_cloud_storage_uris=['gs://{{var.value.revrec_outbound_zone}}/CS_RR2_FEES_Workday.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
    
    t7 = BigQueryOperator(
        task_id='bq_task_outbound_revrec_refund_workday',
        bql='/outbound_revrec_refund_workday.sql',
        destination_dataset_table='LANDING.OUTBOUND_REVREC_PAYMENT_REFUND_WORKDAY',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t8 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_revrec_refund_workday_file',
        source_project_dataset_table='LANDING.OUTBOUND_REVREC_PAYMENT_REFUND_WORKDAY',
        destination_cloud_storage_uris=['gs://{{var.value.revrec_outbound_zone}}/CS_RR2_REFUNDS_Workday.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
	
    t9 = BigQueryOperator(
        task_id='bq_task_outbound_revrec_void_payment_workday',
        bql='/outbound_revrec_void_payment_workday.sql',
        destination_dataset_table='LANDING.OUTBOUND_REVREC_VOID_PAYMENT_WORKDAY',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t10 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_revrec_void_payment_workday_file',
        source_project_dataset_table='LANDING.OUTBOUND_REVREC_VOID_PAYMENT_WORKDAY',
        destination_cloud_storage_uris=['gs://{{var.value.revrec_outbound_zone}}/CS_RR2_VOID_Workday.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)

    t11 = BigQueryOperator(
        task_id='bq_task_outbound_revrec_dues_payment_workday',
        bql='/outbound_revrec_dues_workday.sql',
        destination_dataset_table='LANDING.OUTBOUND_REVREC_PAYMENT_DUES_WORKDAY',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t12 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_revrec_dues_payment_workday_file',
        source_project_dataset_table='LANDING.OUTBOUND_REVREC_PAYMENT_DUES_WORKDAY',
        destination_cloud_storage_uris=['gs://{{var.value.revrec_outbound_zone}}/CS_RR2_DUES_Workday.tmp'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
		
		
    t13 = BashOperator(
        task_id='reformat_file_to_fixed_width',
        bash_command=bash_cmd)
                  
t1 >>t2 >>t3 >>t4 >>t5 >>t6 >>t7 >>t8 >>t9 >>t10 >>t11 >>t12 >>t13
