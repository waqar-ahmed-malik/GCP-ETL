from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o3, 0o1),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}


with DAG('DAG_OUTBOUND_MEMBERSHIP_NOTIFICATION_XML', schedule_interval=None, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/notification_sql') as dag:
    t1 = BigQueryOperator(
        task_id='T1_BQ_WORK_MEM_NOTIFICATION_LOAD',
        bql='/WORK_MEMBERSHIP_NOTIFICATION.sql',
        destination_dataset_table='LANDING.WORK_MEMBERSHIP_NOTIFICATION',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
        
    t2 = BigQueryOperator(
        task_id='T2_BQ_STAGE_MEM_NOTIFICATION_LOAD',
        bql='/STAGE_MEMBERSHIP_NOTIFICATION.sql',
        destination_dataset_table='OPERATIONAL.STAGE_MEMBERSHIP_NOTIFICATION',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t3 = BashOperator(
        task_id='T3_MEMBERSHIP_NOTIFICATION_XML',
        bash_command='python /home/airflow/gcs/data/GCPDWH/outbound/sql_to_xml.py --config "config.properties" --productconfig "sqltoxml.properties" --env "prod" --detailsqlfile "notification_detail_sql_file.sql" --auditsqlfile  "notification_audit_sql_file.sql" --filename "ST"{{ execution_date.strftime("%Y%m%d%H%M%S") }}"000NOTF.xml" --bucketname "dw-prod-connectsuite"')
                                                                                           
t1 >>t2 >>t3
