from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 10),
    'email': ['ashok.ganapa@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}


with DAG('DAG_GCP_CCPA_CUSTOMER_LOAD', schedule_interval='00 16 * * *', catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/ccpa/sql') as dag:
    t1 = BigQueryOperator(
        task_id='LOAD_CCPA_CUSTOMER_DETAILS_MEMBERSHIP',
        bql='/MEMBERSHIP.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
        
    t2 = BigQueryOperator(
        task_id='LOAD_CCPA_CUSTOMER_DETAILS_INSURANCE',
        bql='/INSURANCE.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t3 = BigQueryOperator(
        task_id='LOAD_CCPA_CUSTOMER_DETAILS_CLUB_OWNED_REPAIR',
        bql='/CLUB_OWNED_REPAIR.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
        
    t4 = BigQueryOperator(
        task_id='LOAD_CCPA_CUSTOMER_DETAILS_CONTACT',
        bql='/CCPA_CUSTOMER_CONTACT.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
                                                                                           
t1 >>t2 >>t3 >>t4
