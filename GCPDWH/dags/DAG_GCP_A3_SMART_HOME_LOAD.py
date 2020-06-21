from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

a3_smart_home_config = Variable.get("smart_data", deserialize_json=True)
a3_smart_home_schema = a3_smart_home_config["input_fields"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 16),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = '00 15 * * 0'

# Note the template search path. Airflow, by default searches the DAG directory for query templates. You'll need to tell it otherwise
with DAG('DAG-A3_SMART_HOME-PIPELINE', schedule_interval=schedule_interval, catchup=False, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/a3_smart_home') as dag:

    t1 = GoogleCloudStorageToBigQueryOperator(
        task_id='load_a3_smart_home_file_landing_table',
        bucket='{{ var.json.smart_data.bucket }}',
        source_objects=[
            '{{ var.json.smart_data.directory }}/{{ var.json.smart_data.filename_prefix }}{{ (execution_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}{{ var.json.smart_data.filename_suffix }}'
        ],
        destination_project_dataset_table='{{ var.json.smart_data.target_project }}.{{ var.json.smart_data.target_dataset }}.{{ var.json.smart_data.target_table }}',
        source_format='CSV',
        schema_fields=a3_smart_home_schema,
#        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        field_delimiter=',',
        quote_character='"',
        max_bad_records=100,
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=False)

    t2 = BashOperator(
        task_id='T2_LOAD_MDM_SMART_HOME',
        bash_command='python /home/airflow/gcs/data/GCPDWH/a3_smart_home/mdm_smart_home.py --config "config.properties" --productconfig "travelware.properties" --env {{var.value.environment}}')
     
         
    t3 = BigQueryOperator(
        task_id = 'insert_customer_details',
        bql = '/insert_customer_details.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
    
    t4 = BashOperator(
        task_id='DELETING_FILES',
        bash_command = 'gsutil rm gs://{{ var.json.smart_data.bucket }}/{{ var.json.smart_data.directory }}/{{ var.json.smart_data.filename_prefix }}{{ execution_date.strftime("%Y%m%d") }}{{ var.json.smart_data.filename_suffix }}'
        )
    
t1 >> t2 >> t3 >> t4
    
