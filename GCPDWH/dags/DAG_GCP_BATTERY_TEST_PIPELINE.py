from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

battery_test_config = Variable.get("var_ers_battery_test_results", deserialize_json=True)
battery_test_schema = battery_test_config["input_fields"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 26),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = '00 15 * * 0'

with DAG('DAG_GCP_BATTERY_TEST_PIPELINE', schedule_interval=schedule_interval, catchup=False, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/aar/sql') as dag:

    copy_file_sftp_gcs = BashOperator(
        task_id='copy_data_sftp_gcs',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config config.properties --productconfig battertest.properties --env {{ var.value.environment }}')
    
    work_ers_battery_test_results = GoogleCloudStorageToBigQueryOperator(
        task_id='load_ers_battery_test_results_file_to_raw_table',
        bucket='{{ var.json.var_ers_battery_test_results.bucket }}',
        source_objects=['{{ var.json.var_ers_battery_test_results.directory}}/{{ execution_date.strftime("%Y%m%d") }}{{ var.json.var_ers_battery_test_results.filename_sfx }}'],
        destination_project_dataset_table='{{ var.json.var_ers_battery_test_results.target_project }}.{{ var.json.var_ers_battery_test_results.target_dataset }}.{{ var.json.var_ers_battery_test_results.target_table }}',
        source_format='CSV',
        schema_fields=battery_test_schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        field_delimiter='|',
        quote_character='"',
        max_bad_records=100,
        ignore_unknown_values= False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect= False)    


copy_file_sftp_gcs >> work_ers_battery_test_results
