from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

ers_repair_shop_portal_config = Variable.get("ers_repair_shop_portal", deserialize_json=True)
ers_repair_shop_portal_schema = ers_repair_shop_portal_config["input_fields"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o7, 31),
    'email': ['atul.guleria@norcal.aaa.com'],  # ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval='00 21 * * *'

# Note the template search path. Airflow, by default searches the DAG directory for query templates. You'll need to tell it otherwise
with DAG('DAG_GCS_ERS_REPAIR_SHOP_PORTAL', schedule_interval=schedule_interval, catchup=True, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/ersrepairshopportal/sql') as dag:

#  for i,value in enumerate(all_files):
    raw_worker = GoogleCloudStorageToBigQueryOperator(
        task_id='load_ersrepairshopportal_file_to_raw_table',
        bucket='{{ var.json.ers_repair_shop_portal.bucket }}',
        source_objects=[
            '{{ var.json.ers_repair_shop_portal.directory }}/RSP_005_{{ (execution_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}.txt'
        ],
        destination_project_dataset_table='{{ var.json.ers_repair_shop_portal.target_project }}.{{ var.json.ers_repair_shop_portal.target_dataset }}.{{ var.json.ers_repair_shop_portal.target_table }}',
        source_format='CSV',
        schema_fields=ers_repair_shop_portal_schema,
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
 
    load_ersrepairshopportal = BigQueryOperator(
        task_id='load_ersrepairshopportal',
        bql='/ers_repair_shop_portal_load.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')

    raw_worker >> load_ersrepairshopportal


