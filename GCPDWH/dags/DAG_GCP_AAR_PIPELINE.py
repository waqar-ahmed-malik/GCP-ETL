from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

aar_config = Variable.get("work_aar_garage_inspection", deserialize_json=True)
aaa_schema = aar_config["input_fields"]

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
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = '00 17 * * 0'
# Temp schedule interval for testing

# Note the template search path. Airflow, by default searches the DAG directory for query templates. You'll need to tell it otherwise
with DAG('DAG_GCP_AAR_PIPELINE', schedule_interval=None, catchup=False, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/aar/sql') as dag:
    
    copy_file_sftp_gcs = BashOperator(
        task_id='copy_data_sftp_gcs',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config config.properties --productconfig aar.properties --env {{ var.value.environment }}')
    # Raw tables are used to isolate file issues from the daily history tables. 
    
    work_aar_garage = GoogleCloudStorageToBigQueryOperator(
        task_id='load_aar_garage_inspection_file_to_raw_table',
        bucket='{{ var.json.work_aar_garage_inspection.bucket }}',
        source_objects=['{{ var.json.work_aar_garage_inspection.directory}}/{{ var.json.work_aar_garage_inspection.filename }}'
        ],
        destination_project_dataset_table='{{ var.json.work_aar_garage_inspection.target_project }}.{{ var.json.work_aar_garage_inspection.target_dataset }}.{{ var.json.work_aar_garage_inspection.target_table }}',
        source_format='CSV',
        schema_fields=aaa_schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        field_delimiter=',',
        quote_character='"',
        max_bad_records=100,
        ignore_unknown_values= False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect= False)    

    customer_product_aar_garage_inspection = BigQueryOperator(
        task_id='operational_aar_garage_inspection',
        bql='/AAR_GARAGE_INSPECTION.sql',
#        destination_dataset_table='{{ var.json.work_aar_garage_inspection.target_project }}.{{ var.json.work_aar_garage_inspection.final_dataset }}.{{ var.json.work_aar_garage_inspection.final_table }}',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
#        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn') 

copy_file_sftp_gcs >> work_aar_garage >> customer_product_aar_garage_inspection
