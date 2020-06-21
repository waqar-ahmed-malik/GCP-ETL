from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o3, 0o1),
    'email': ['kishore.boddu@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = "00 13 * * 1-5"

with DAG('DAG_GCP_OUTBOUND_IG_PAYMENT_CENTRAL_FILE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/ig_payment_central') as dag:
    
    t1 = BigQueryOperator(
        task_id='bq_work_stage_pos_ig_pc_feed',
        bql='/work_stage_pos_insurance_payments.sql',
        destination_dataset_table='LANDING.WORK_STAGE_POS_INSURANCE_PAYMENTS',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
    t2 = BigQueryOperator(
        task_id='bq_stage_pos_ig_pc_feed',
        bql='/stage_pos_insurance_payments.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
        
    t3 = BigQueryOperator(
        task_id='bq_task_stg_outbound_ig_pc_feed',
        bql='/outbound_pos_payment_central.sql',
        destination_dataset_table='LANDING.OUTBOUND_STAGE_PAYMENT_CENTRAL',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
    
                
    t4 = BigQueryToCloudStorageOperator(
        task_id='bq_task_to_generate_ig_payment_central_dat_file',
        source_project_dataset_table='LANDING.OUTBOUND_STAGE_PAYMENT_CENTRAL',
        destination_cloud_storage_uris=['gs://{{var.value.pc_feed_outbound_zone}}/{{ execution_date.strftime("%Y%m%d_%H%M%S") }}_PMT_E_NCNUCLUB_PMTCTRL_7022_D.fix'],
        export_format='CSV',
        bigquery_conn_id='aaa_bq_conn',
        field_delimiter='|',
        print_header=False)
    
    
t1 >>t2 >>t3 >>t4
