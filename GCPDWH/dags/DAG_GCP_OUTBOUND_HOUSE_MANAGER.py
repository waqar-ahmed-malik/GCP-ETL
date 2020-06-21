from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 10),
    'email': ['ashok.ganapa@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}


with DAG('DAG_GCP_OUTBOUND_HOUSE_MANAGER', schedule_interval='00 10 * * *', catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/house_manager') as dag:
    t1 = BigQueryOperator(
        task_id='LOAD_OPERATIONAL_HOUSE_MANAGER_PAYMENTS',
        bql='/OPERATIONAL_HOUSE_MANAGER_PAYMENTS.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
        
    t2 = BigQueryOperator(
        task_id='LOAD_OPERATIONAL_HOUSE_MANAGER_REFUNDS',
        bql='/OPERATIONAL_HOUSE_MANAGER_REFUNDS.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t3 = BigQueryOperator(
        task_id='LOAD_OUTBOUND_HOUSE_MANAGER_PAYMENTS',
        bql='/OUTBOUND_HOUSE_MANAGER_PAYMENTS.sql',
        destination_dataset_table='LANDING.OUTBOUND_HOUSE_MANAGER_PAYMENTS',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t4 = BigQueryToCloudStorageOperator(
        task_id='GENERATE_OUTBOUND_HOUSE_MANAGER_PAYMENTS_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_HOUSE_MANAGER_PAYMENTS',
        destination_cloud_storage_uris=['gs://{{var.value.house_manager_outbound_zone}}/current/HOUSE_MANGER_PAYMENTS_{{ds_nodash}}.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=True)
    
    t5 = BigQueryToCloudStorageOperator(
        task_id='ARCHIVE_OUTBOUND_HOUSE_MANAGER_PAYMENTS_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_HOUSE_MANAGER_PAYMENTS',
        destination_cloud_storage_uris=['gs://{{var.value.outbound_archive_zone}}/bl-house-manager/HOUSE_MANGER_PAYMENTS_{{ds_nodash}}.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=True)
    
    t6 = BigQueryOperator(
        task_id='LOAD_OUTBOUND_HOUSE_MANAGER_REFUNDS',
        bql='/OUTBOUND_HOUSE_MANAGER_REFUNDS.sql',
        destination_dataset_table='LANDING.OUTBOUND_HOUSE_MANAGER_REFUNDS',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t7 = BigQueryToCloudStorageOperator(
        task_id='GENERATE_OUTBOUND_HOUSE_MANAGER_REFUNDS_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_HOUSE_MANAGER_REFUNDS',
        destination_cloud_storage_uris=['gs://{{var.value.house_manager_outbound_zone}}/current/HOUSE_MANGER_PAYMENT_REFUNDS_{{ds_nodash}}.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=True)
    
    t8 = BigQueryToCloudStorageOperator(
        task_id='ARCHIVE_OUTBOUND_HOUSE_MANAGER_REFUNDS_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_HOUSE_MANAGER_REFUNDS',
        destination_cloud_storage_uris=['gs://{{var.value.outbound_archive_zone}}/bl-house-manager/HOUSE_MANGER_PAYMENT_REFUNDS_{{ds_nodash}}.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=True)
                                                                                           
t1 >>t2 >>t3 >>t4 >>t5 >>t6 >>t7 >>t8
