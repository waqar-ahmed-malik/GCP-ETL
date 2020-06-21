from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2014, 0o1, 0o1),
    'email': ['william.moore@norcal.aaa.com'],  # ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = timedelta(hours=24)

with DAG('Workday-Dev-to-Prod', schedule_interval=schedule_interval, catchup=True, default_args=default_args) as dag:
    t0 = DummyOperator(task_id='dummy')
    t1 = GoogleCloudStorageToBigQueryOperator(
        task_id='workday_costcenter',
        bucket='{{ var.json.workday_costcenter.bucket }}',
        source_objects=[
            '{{ var.json.workday_costcenter.directory }}/{{ var.json.workday_costcenter.target_dataset }}.{{ var.json.workday_costcenter.target_table }}{{ ds_nodash }}'
        ],
        destination_project_dataset_table='{{ var.json.workday_costcenter.target_project }}.{{ var.json.workday_costcenter.target_dataset }}.{{ var.json.workday_costcenter.target_table }}{{ ds_nodash }}',
        source_format='AVRO',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=True)
    t2 = GoogleCloudStorageToBigQueryOperator(
        task_id='workday_job_codes',
        bucket='{{ var.json.workday_job_codes.bucket }}',
        source_objects=[
            '{{ var.json.workday_job_codes.directory }}/{{ var.json.workday_job_codes.target_dataset }}.{{ var.json.workday_job_codes.target_table }}{{ ds_nodash }}'
        ],
        destination_project_dataset_table='{{ var.json.workday_job_codes.target_project }}.{{ var.json.workday_job_codes.target_dataset }}.{{ var.json.workday_job_codes.target_table }}{{ ds_nodash }}',
        source_format='AVRO',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=True)
    t3 = GoogleCloudStorageToBigQueryOperator(
        task_id='workday_locations',
        bucket='{{ var.json.workday_locations.bucket }}',
        source_objects=[
            '{{ var.json.workday_locations.directory }}/{{ var.json.workday_locations.target_dataset }}.{{ var.json.workday_locations.target_table }}{{ ds_nodash }}'
        ],
        destination_project_dataset_table='{{ var.json.workday_locations.target_project }}.{{ var.json.workday_locations.target_dataset }}.{{ var.json.workday_locations.target_table }}{{ ds_nodash }}',
        source_format='AVRO',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=True)
    t4 = GoogleCloudStorageToBigQueryOperator(
        task_id='workday_market_hierarchy',
        bucket='{{ var.json.workday_market_hierarchy.bucket }}',
        source_objects=[
            '{{ var.json.workday_market_hierarchy.directory }}/{{ var.json.workday_market_hierarchy.target_dataset }}.{{ var.json.workday_market_hierarchy.target_table }}{{ ds_nodash }}'
        ],
        destination_project_dataset_table='{{ var.json.workday_market_hierarchy.target_project }}.{{ var.json.workday_market_hierarchy.target_dataset }}.{{ var.json.workday_market_hierarchy.target_table }}{{ ds_nodash }}',
        source_format='AVRO',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=True)
    t5 = GoogleCloudStorageToBigQueryOperator(
        task_id='workday_supervisory_hierarchy',
        bucket='{{ var.json.workday_supervisory_hierarchy.bucket }}',
        source_objects=[
            '{{ var.json.workday_supervisory_hierarchy.directory }}/{{ var.json.workday_supervisory_hierarchy.target_dataset }}.{{ var.json.workday_supervisory_hierarchy.target_table }}{{ ds_nodash }}'
        ],
        destination_project_dataset_table='{{ var.json.workday_supervisory_hierarchy.target_project }}.{{ var.json.workday_supervisory_hierarchy.target_dataset }}.{{ var.json.workday_supervisory_hierarchy.target_table }}{{ ds_nodash }}',
        source_format='AVRO',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=True)
    t6 = GoogleCloudStorageToBigQueryOperator(
        task_id='workday_worker',
        bucket='{{ var.json.workday_worker.bucket }}',
        source_objects=[
            '{{ var.json.workday_worker.directory }}/{{ var.json.workday_worker.target_dataset }}.{{ var.json.workday_worker.target_table }}{{ ds_nodash }}'
        ],
        destination_project_dataset_table='{{ var.json.workday_worker.target_project }}.{{ var.json.workday_worker.target_dataset }}.{{ var.json.workday_worker.target_table }}{{ ds_nodash }}',
        source_format='AVRO',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=True)
        
    t0 >> [t1,t2,t3,t4,t5,t6]