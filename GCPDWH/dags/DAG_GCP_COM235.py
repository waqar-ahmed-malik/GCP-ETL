from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


com235_config = Variable.get("com235", deserialize_json=True)

com235_schema = com235_config["input_fields"]


default_args = {

    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 5),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'], 
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)

}


schedule_interval = '00 01 2-6,16-21 * *'


with DAG('DAG_GCP_COM235', schedule_interval=schedule_interval, catchup=False, default_args=default_args,
         template_searchpath='/home/airflow/gcs/data/GCPDWH/insurance/sql') as dag:
    
    
    t1 = GoogleCloudStorageToBigQueryOperator(
        task_id='load_com235_file_to_raw_table',
        bucket='{{ var.json.com235.bucket }}',
        source_objects=[
            '{{ var.json.com235.directory }}/{{ var.json.com235.filename_prefix }}.{{ execution_date.strftime("%m%d%y") }}*'
        ],
        destination_project_dataset_table='{{ var.json.com235.target_project }}.{{ var.json.com235.target_dataset }}.{{ var.json.com235.target_table }}',
        source_format='CSV',
        schema_fields=com235_schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter=',',
        quote_character='"',
        max_bad_records=100,
        ignore_unknown_values=False,
        google_cloud_storage_conn_id='aaa_bq_conn',
        bigquery_conn_id='aaa_bq_conn',
        autodetect=False)
    
    t2 = BigQueryOperator(
        task_id='raw_table_to_com235_daily',
        bql='/com235_load.sql',
        destination_dataset_table='{{var.value.project}}.CUSTOMER_PRODUCT.LIFE_COM235',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    
    t1>>t2
