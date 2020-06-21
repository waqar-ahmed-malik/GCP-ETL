import airflow
import logging

from airflow import DAG
from google.cloud import storage
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
# from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 10),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

logging.info(Variable.get("environment"))

def delete_from_gcs(**kwargs):
    storage_client = storage.Client(project =Variable.get("project"))
    blobs = storage_client.get_bucket('dw-' + Variable.get("environment") + '-outbound')
    blobs = blobs.list_blobs(prefix='workday-ams-gl/ams_gl_',
                             delimiter='/')
    for blob in blobs:
        if blob.name[-3:] == 'csv':
            bucket = storage_client.get_bucket('dw-' + Variable.get("environment") + '-outbound')
            blob = bucket.blob(blob.name)
            blob.delete()
            print(blob.name)


date_time_stamp = str(datetime.now()).replace('-', '').replace(' ', '').replace(':', '')[:14]

with DAG('AMS_OPERATIONAL_GENERAL_LEDGER_AND_OUTBOUND_FILE_LOAD', schedule_interval=None, catchup=False,
         default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/ams/sql') as dag:

    delete_daily_files = PythonOperator(
        task_id='delete_previous_days_files',
        python_callable=delete_from_gcs,
        dag=dag)

    t1 = BigQueryOperator(
        task_id='ams_operational_table_load_from_landing',
        bql='/OPERATIONAL_AMS_GENERAL_LEDGER.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_GENERAL_LEDGER',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t2 = BigQueryOperator(
        task_id='bq_task_ams_gl_109',
        bql='/AMS_GL_109.sql',
        destination_dataset_table='{{var.value.project}}.LANDING.OUTBOUND_AMS_GL_109',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t3 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_gl_109_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_AMS_GL_109',
        destination_cloud_storage_uris=['gs://dw-{{var.value.environment}}-outbound/workday-ams-gl/ams_gl_109_' + date_time_stamp + '.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',',
        print_header=True)

    t4 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_gl_109_csv_file_archive',
        source_project_dataset_table='LANDING.OUTBOUND_AMS_GL_109',
        destination_cloud_storage_uris=['gs://dw-{{var.value.environment}}-outbound-archive/workday-ams-gl/ams_gl_109_' + date_time_stamp + '.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',',
        print_header=True)

    t5 = BigQueryOperator(
        task_id='bq_task_ams_gl_107',
        bql='/AMS_GL_107.sql',
        destination_dataset_table='{{var.value.project}}.LANDING.OUTBOUND_AMS_GL_107',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t6 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_gl_107_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_AMS_GL_107',
        destination_cloud_storage_uris=['gs://dw-{{var.value.environment}}-outbound/workday-ams-gl/ams_gl_107_' + date_time_stamp + '.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',',
        print_header=True)

    t7 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_gl_107_csv_file_archive',
        source_project_dataset_table='LANDING.OUTBOUND_AMS_GL_107',
        destination_cloud_storage_uris=['gs://dw-{{var.value.environment}}-outbound-archive/workday-ams-gl/ams_gl_107_' + date_time_stamp + '.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',',
        print_header=True)

    t8 = BigQueryOperator(
        task_id='bq_task_ams_gl_100',
        bql='/AMS_GL_100.sql',
        destination_dataset_table='{{var.value.project}}.LANDING.OUTBOUND_AMS_GL_100',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t9 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_gl_100_csv_file',
        source_project_dataset_table='LANDING.OUTBOUND_AMS_GL_100',
        destination_cloud_storage_uris=['gs://dw-{{var.value.environment}}-outbound/workday-ams-gl/ams_gl_100_' + date_time_stamp + '.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',',
        print_header=True)

    t10 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_gl_100_csv_file_archive',
        source_project_dataset_table='LANDING.OUTBOUND_AMS_GL_100',
        destination_cloud_storage_uris=['gs://dw-{{var.value.environment}}-outbound-archive/workday-ams-gl/ams_gl_100_' + date_time_stamp + '.csv'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter=',',
        print_header=True)

delete_daily_files >> t1
t1 >> [t2, t5, t8]
t2 >> [t3, t4]
t5 >> [t6, t7]
t8 >> [t9, t10]