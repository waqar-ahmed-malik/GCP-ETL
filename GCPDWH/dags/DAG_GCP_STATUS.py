from airflow import DAG
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 4, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = "30 20,8 * * *"

with DAG('DAG_GCP_STATUS', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    
    


 t1 = MySqlToGoogleCloudStorageOperator(
     
    task_id='airlflow_dag_status',
    mysql_conn_id='airflow_db',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    sql='SELECT * FROM task_instance',
    bucket='dw-prod-inbound',
    filename='airflow_status.json',
    field_delimiter = ',',
    dag=dag)
 
 


t3 = GoogleCloudStorageToBigQueryOperator(
        task_id="load_table",
        bigquery_conn_id='aaa_bq_conn',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        bucket='dw-prod-inbound',
        destination_project_dataset_table="LANDING.airflow_status",
        source_objects=['airflow_status.json'],
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        dag=dag)


t1>>t3
