from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

adobe_config = Variable.get("adobe_pipeline", deserialize_json=True)
adobe_schema = adobe_config["input_fields"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 11),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = '00 17 * * *'

# Note the template search path. Airflow, by default searches the DAG directory for query templates. You'll need to tell it otherwise
with DAG('DAG-ADOBE-PIPELINE', schedule_interval=schedule_interval, catchup=False, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/adobe/sql') as dag:

    raw_adobe = GoogleCloudStorageToBigQueryOperator(
        task_id='load_adobe_file_to_raw_table',
        bucket='{{ var.json.adobe_pipeline.bucket }}',
        source_objects=[
            '{{ var.json.adobe_pipeline.directory }}/{{ var.json.adobe_pipeline.filename_prefix }}{{ execution_date.strftime("%Y-%m-%d") }}{{ var.json.adobe_pipeline.filename_suffix }}'
        ],
        destination_project_dataset_table='{{ var.json.adobe_pipeline.target_project }}.{{ var.json.adobe_pipeline.target_dataset }}.{{ var.json.adobe_pipeline.target_table }}',
#         {{ ds_nodash }}',
        source_format='CSV',
        schema_fields=adobe_schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter='\t',
        quote_character='"',
        max_bad_records=100,
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=False)
    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.
    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  
#     delete_adobe_daily_history = BigQueryOperator(
#         task_id='delete_from_adobe_landing_table',
# #         bql='DELETE FROM LANDING.ADOBE_CAMPAIGN_DATA_DAILY_HISTORY WHERE fileid = {{ ds_nodash }}',
#         bql='DELETE FROM LANDING.WORK_ADOBE_CAMPAIGN_DATA WHERE TRUE',
#         use_legacy_sql=False,
#         bigquery_conn_id='aaa_bq_conn')
#         
#     adobe_daily_history = BigQueryOperator(
#         task_id='raw_table_to_adobe_daily_history',
#         bql='/raw_table_to_adobe_daily_history.sql',
#         destination_dataset_table='{{var.value.project}}.LANDING.WORK_ADOBE_CAMPAIGN_DATA',
#         use_legacy_sql=False,
#         write_disposition='WRITE_APPEND',
#         bigquery_conn_id='aaa_bq_conn')

    adobe_campain_data = BigQueryOperator(
        task_id='digital_marketing_adobe_campaign_data',
        bql='/digital_marketing_adobe_campaign_data.sql',
        destination_dataset_table='{{var.value.project}}.DIGITAL_MARKETING.ADOBE_CAMPAIGN_DATA',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')
     
    clickstream_hit_detail = BigQueryOperator(
        task_id='digital_marketing_clickstream_hit_detail',
        bql='/clickstream_hit_detail_insert.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
  
    clickstream_membership_summary = BigQueryOperator(
        task_id='digital_marketing_clickstream_membership_summary',
        bql='/clickstream_membership_summary_insert.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
  
    clickstream_visit_summary = BigQueryOperator(
        task_id='digital_marketing_clickstream_visit_summary',
        bql='/clickstream_visit_summary_insert.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
  
    clickstream_visitor = BigQueryOperator(
        task_id='digital_marketing_clickstream_visitor',
        bql='/clickstream_visitor_insert.sql',
        use_legacy_sql=False,
        bigquery_conn_id='aaa_bq_conn')
     
    delete_files = BashOperator(
        task_id='Detele_file_after_loading',
        bash_command = 'gsutil rm gs://{{ var.json.adobe_pipeline.bucket }}/{{ var.json.adobe_pipeline.directory }}/{{ var.json.adobe_pipeline.filename_prefix }}{{ execution_date.strftime("%Y-%m-%d") }}{{ var.json.adobe_pipeline.filename_suffix }}'
        )

    raw_adobe >> adobe_campain_data >> clickstream_hit_detail >> clickstream_membership_summary >> clickstream_visit_summary >> clickstream_visitor >> delete_files