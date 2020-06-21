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
    'email': ['kishore.boddu@norcal.aaa.com'],  # ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = "00 15 * * 2-7"

with DAG('DAG_GCP_OUTBOUND_POS_FILES_LOAD', schedule_interval=schedule_interval, catchup=False,
         default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/pos') as dag:
    t1 = BigQueryOperator(
        task_id='bq_task_pos_tender_pickup',
        bql='/outbound_pos_tender_pickup.sql',
        destination_dataset_table='LANDING.OUTBOUND_POS_TENDER_PICKUP',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t2 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_pos_tender_pickup_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_POS_TENDER_PICKUP',
        destination_cloud_storage_uris=['gs://{{var.value.pos_outbound_zone}}/{{ds_nodash}}_tender_pickup.txt'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='	',
        print_header=True)

    t3 = BigQueryOperator(
        task_id='bq_task_pos_bank_deposit',
        bql='/outbound_pos_bank_deposit.sql',
        destination_dataset_table='LANDING.OUTBOUND_POS_BANK_DEPOSIT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t4 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_pos_bank_deposit_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_POS_BANK_DEPOSIT',
        destination_cloud_storage_uris=['gs://{{var.value.pos_outbound_zone}}/{{ds_nodash}}_bank_deposit.txt'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='	',
        print_header=True)

    t5 = BigQueryOperator(
        task_id='bq_task_pos_tender_summary',
        bql='/outbound_pos_tender_summary.sql',
        destination_dataset_table='LANDING.OUTBOUND_POS_TENDER_SUMMARY',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t6 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_pos_tender_summary_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_POS_TENDER_SUMMARY',
        destination_cloud_storage_uris=['gs://{{var.value.pos_outbound_zone}}/{{ds_nodash}}_tender_summary.txt'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='	',
        print_header=True)

    t7 = BigQueryOperator(
        task_id='bq_task_pos_transaction_detail',
        bql='/outbound_pos_transaction_detail.sql',
        destination_dataset_table='LANDING.OUTBOUND_POS_TRANSACTION_DETAIL',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

    t8 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_pos_transaction_detail_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_POS_TRANSACTION_DETAIL',
        destination_cloud_storage_uris=['gs://{{var.value.pos_outbound_zone}}/{{ds_nodash}}_transaction_detail.txt'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='	',
        print_header=True)    

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
