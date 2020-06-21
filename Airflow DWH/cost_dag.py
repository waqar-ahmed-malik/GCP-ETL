from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.models import Variable
import os
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 8),
    'email': ['mohammed.a.saeed@usps.gov'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = '0 17 * * *'

sftp_source_directory = ''
source_file_extension = '.xlsx'
sheet_name = "Master List"

data_source = 'cost'
project_id = Variable.get('big-query-project')
bucket = Variable.get('gcs-bucket')


def get_list_of_current_folder_csv_files(**context):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    context['ti'].xcom_push(key='files_list', value = ['{}/current/{}'.format(data_source, blob.name) for blob in bucket.list_blobs(prefix='{}/current'.format(data_source))]
        




with DAG('cost_dag.py', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    t1=BashOperator(
    task_id='clean staging folder',
    bash_command=('python /home/airflow/gcs/data/utilities/clean_staging_folder.py '
                 '--data_source="{}" '
                 '--bucket="{}" ').format(data_source, bucket)
        )

    t2=BashOperator(
    task_id='transfer_file_from_sftp_to_gcs_bucket',
    bash_command=('python /home/airflow/gcs/data/utilities/sftp_migration.py '
                 '--source_directory="{}" '
                 '--data_source="{}" '
                 '--sftp_host="{}" '
                 '--sftp_username="{}" '
                 '--sftp_password="{}" '
                 '--bucket="{}" ').format(sftp_source_directory, data_source, sftp_connection.host, sftp_connection.login, sftp_connection.password, bucket)
    )

    t3 = BashOperator(
    task_id='convert_excel_to_csv',
    bash_command=('python /home/airflow/gcs/data/utilities/excel_to_csv.py '
                 '--data_source="{}" '
                 '--source_file_extension="{}" '
                 '--bucket="{}" '
                '--sheet_name="{}" ').format(data_source, source_file_extension, bucket, sheet_name)
    )


    t4 = BashOperator(
    task_id='transfer_from_staging_to_current',
    bash_command=('python /home/airflow/gcs/data/utilities/staging_to_current.py '
                 '--data_source="{}" '
                 '--bucket="{}" ').format(data_source, bucket)
    )

    t5 = PythonOperator(
    task_id='get_list_of_current_folder_csv_files',
    python_callable= get_list_of_current_folder_csv_files,
    provide_context=True
    )


    t6 = GoogleCloudStorageToBigQueryOperator(
        task_id='land_data_into_big_query',
        bucket=bucket,
        provide_context=True
        source_objects=context['ti'].xcom_pull(key='files_list'),
        destination_project_dataset_table='{}.landing.{}'.format(project_id, data_source),
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter=',',
        quote_character='"',
        max_bad_records=0,
        ignore_unknown_values=False,
        autodetect=False,
        allow_quoted_newlines = True,
        schema_object = '{}/current/schema.json'.format(data_source)
        )


    t7 = BigQueryOperator(
        task_id='create_operational_{}'.format(data_source),
        sql='sources/{}/Operational/SQL/DDL/create_operational_{}.sql'.format(data_source, data_source),
        use_legacy_sql=False)


    t8 = BigQueryOperator(
        task_id='landing_to_operational_{}'.format(data_source),
        sql='sources/{}/Operational/SQL/DML/landing_to_operational_{}.sql'.format(data_source, data_source),
        use_legacy_sql=False)


    t9 = BigQueryOperator(
    task_id='create_data_warehouse_view',
    sql='views/warehouse/create_dim_{}.sql'.format(data_source),
    use_legacy_sql=False,
    )

    t10 = BashOperator(
    task_id='audit',
    bash_command=('python /home/airflow/gcs/data/utilities/audit.py '
                 '--data_source="{}" '
                 '--bucket="{}" '
                 '--bq_project_id="{}" ').format(data_source, bucket, project_id)
    )
    
    t11 = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='archive_files',
    source_bucket=bucket,
    source_object=gcs_current_csv_file_path,
    destination_bucket=bucket,
    move_object=True,
    destination_object=gcs_archive_csv_file_path)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11