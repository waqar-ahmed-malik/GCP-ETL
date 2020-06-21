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


with DAG('DAG_GCP_OUTBOUND_MEMBERSHIP_BILL2', schedule_interval=None, catchup=False, default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/outbound/sql/bill2') as dag:
    t1 = BigQueryOperator(
        task_id='LOAD_STAGE_MEMBERSHIP_BILL2',
        bql='/work_stg_membership_bill2.sql',
        destination_dataset_table='LANDING.WORK_STG_MEMBERSHIP_BILL2',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='aaa_bq_conn')
        
    t2 = BigQueryOperator(
        task_id='LOAD_BILL2_DATA_TO_STAGE_MEMBERSHIP_BILL',
        bql='/stage_membership_bill2.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t3 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE1',
        bql='/outbound_bill2_exception_rule1.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t4 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE2',
        bql='/outbound_bill2_exception_rule2.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t5 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE3',
        bql='/outbound_bill2_exception_rule3.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t6 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE4',
        bql='/outbound_bill2_exception_rule4.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t7 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE5',
        bql='/outbound_bill2_exception_rule5.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t8 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE6',
        bql='/outbound_bill2_exception_rule6.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t9 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE7',
        bql='/outbound_bill2_exception_rule7.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t10 = BigQueryOperator(
        task_id='UPDATE_BILL2_EXCEPTION_RULE8',
        bql='/outbound_bill2_exception_rule8.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
    t11 = BashOperator(
        task_id='BILL2_RAISE_AUDIT_EXCEPTION',
        bash_command='python /home/airflow/gcs/data/GCPDWH/outbound/outbound_audit_bill.py --config "config.properties" --productconfig "sqltoxml.properties" --env "prod" --auditsql "bill2\outbound_bill2_audit_check.sql" --auditfile "BILL2"')
       
    t12 = BashOperator(
        task_id='GENERATE_MEMBERSHIP_BILL2_XML_FILE',
        bash_command='python /home/airflow/gcs/data/GCPDWH/outbound/generate_bill_file_from_bigquery_to_xml.py --config "config.properties" --productconfig "sqltoxml.properties" --env "prod" --detailsqlfile "/bill2/outbound_bill2_detail_file.sql" --auditsqlfile  "/bill2/outbound_bill2_audit_file.sql" --filename "ST{{ execution_date.strftime("%Y%m%d%H%M%S") }}0022BLL.xml" --bucketname "dw-prod-outbound" --foldername "bill2/current/" --hierarchyflag "1"')
                
    t13 = BigQueryOperator(
        task_id='UPDATE_BILL2_FILE_GENERATED',
        bql='/outbound_bill2_update_file_generated.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
            
    t14 = BigQueryOperator(
        task_id='UPDATE_BILL2_PRNT_DT',
        bql='/outbound_bill2_update_bill_prnt_dt.sql',
        destination_dataset_table=False,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='aaa_bq_conn')
    
                                                                                                  
t1 >>t2 >>t3 >>t4 >>t5 >>t6 >>t7 >>t8 >>t9 >>t10 >>t11 >>t12 >>t13 >>t14
