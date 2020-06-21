from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o7, 26),
    'email': ['naren.nanchari@norcal.aaa.com'],  
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


schedule_interval = "00 14 * * 1-5"


with DAG('DAG_GCP_OUTBOUND_CS_EXTERNAL_PAYMENT', schedule_interval=schedule_interval, catchup=False, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/ext_payment') as dag:

    t1 = BigQueryOperator(
        task_id='bq_task_1',
        bql='/Work_outbound_cs_external_payment.sql',
        destination_dataset_table='LANDING.OUTBOUND_CS_EXTERNAL_PAYMENT',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')

   
    t2 = BigQueryToCloudStorageOperator(
        task_id='bq_gcs_csv',
        source_project_dataset_table='LANDING.OUTBOUND_CS_EXTERNAL_PAYMENT',
        destination_cloud_storage_uris=['gs://{{var.value.outbound_cs_external_payments}}/ext_payments_{{ execution_date.strftime("%Y%m%d") }}.csv'],
        export_format='CSV',
        print_header=False,
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',')

t1 >>t2
