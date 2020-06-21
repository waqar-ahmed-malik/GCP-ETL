from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

cor_survey_config = Variable.get("cor_survey", deserialize_json=True)
cor_survey_schema = cor_survey_config["input_fields"]


ers_survey_config = Variable.get("ers_survey", deserialize_json=True)
ers_survey_schema = ers_survey_config["input_fields"]

travel_survey_config = Variable.get("travel_survey", deserialize_json=True)
travel_survey_schema = travel_survey_config["input_fields"]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o7, 25),
    'email': ['ramneek.kaur@norcal.aaa.com'],  # ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}



schedule_interval="30 12 * * *"


with DAG('DAG_GCP_SURVEY_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/qualtrics') as dag:
    
    
    raw_cor_survey = GoogleCloudStorageToBigQueryOperator(
        task_id='load_cor_survey_file_to_raw_table',
        bucket='{{ var.json.cor_survey.bucket }}',
        source_objects=[
            '{{ var.json.cor_survey.directory }}/{{ var.json.cor_survey.filename_prefix }}{{ (execution_date - macros.timedelta(1)).strftime("%Y%m%d") }}{{ var.json.cor_survey.filename_suffix }}'
        ],
        destination_project_dataset_table='{{ var.json.cor_survey.target_project }}.{{ var.json.cor_survey.target_dataset }}.{{ var.json.cor_survey.target_table }}',
        source_format='CSV',
        schema_fields=cor_survey_schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter='|',
        quote_character='"',
        max_bad_records=100,
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=False)
    # Raw tables are used to isolate file issues from the daily history tables. 
    
    cor_survey = BigQueryOperator(
        task_id='load_cor_survey',
        bql='/cor.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
    
      
      
    raw_travel_survey = GoogleCloudStorageToBigQueryOperator(
        task_id='load_travel_survey_file_to_raw_table',
        bucket='{{ var.json.travel_survey.bucket }}',
        source_objects=[
            '{{ var.json.travel_survey.directory }}/{{ var.json.travel_survey.filename_prefix }}{{ (execution_date - macros.timedelta(1)).strftime("%Y%m%d") }}{{ var.json.travel_survey.filename_suffix }}'
        ],
        destination_project_dataset_table='{{ var.json.travel_survey.target_project }}.{{ var.json.travel_survey.target_dataset }}.{{ var.json.travel_survey.target_table }}',
        source_format='CSV',
        schema_fields=travel_survey_schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter='|',
        quote_character='"',
        max_bad_records=100,
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=False)
    
    travel_survey = BigQueryOperator(
        task_id='load_travel_survey',
        bql='/travel.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
#      
#     
#      
    raw_ers_survey = GoogleCloudStorageToBigQueryOperator(
        task_id='load_ers_survey_file_to_raw_table',
        bucket='{{ var.json.ers_survey.bucket }}',
        source_objects=[
            '{{ var.json.ers_survey.directory }}/{{ var.json.ers_survey.filename_prefix }}{{ (execution_date - macros.timedelta(1)).strftime("%Y%m%d") }}{{ var.json.ers_survey.filename_suffix }}'
        ],
        destination_project_dataset_table='{{ var.json.ers_survey.target_project }}.{{ var.json.ers_survey.target_dataset }}.{{ var.json.ers_survey.target_table }}',
        source_format='CSV',
        schema_fields=ers_survey_schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter='|',
        quote_character='"',
        max_bad_records=1000,
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=False)
    
    ers_survey = BigQueryOperator(
        task_id='load_ers_survey',
        bql='/ers.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
#     
    raw_cor_survey>>cor_survey>>raw_travel_survey>>travel_survey>>raw_ers_survey>>ers_survey

    
