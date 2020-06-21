from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o6, 26),
    'email': ['prerna.anand@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval='00 18 11 * *'

with DAG('DAG_GCP_LEXIS_NEXIS_INDIVIDUALS', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t_initial = BashOperator(
        task_id='T_initial_DELETE_LEXIS_NEXIS_DATA',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/delete_lexis_nexis_data_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1')
    
    t0 = BashOperator(
        task_id='T0_LOAD_LEXIS_NEXIS_0',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 0 --landingtablename "WORK_LEXIS_NEXIS_0"')
    
    t1 = BashOperator(
        task_id='T1_LOAD_LEXIS_NEXIS_1',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 1 --landingtablename "WORK_LEXIS_NEXIS_1"')
    
    t2 = BashOperator(
        task_id='T2_LOAD_LEXIS_NEXIS_2',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 2 --landingtablename "WORK_LEXIS_NEXIS_2"')
    
    t3 = BashOperator(
        task_id='T3_LOAD_LEXIS_NEXIS_3',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 3 --landingtablename "WORK_LEXIS_NEXIS_3"')
    
    t4 = BashOperator(
        task_id='T4_LOAD_LEXIS_NEXIS_4',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 4 --landingtablename "WORK_LEXIS_NEXIS_4"')
    
    t5 = BashOperator(
        task_id='T5_LOAD_LEXIS_NEXIS_5',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 5 --landingtablename "WORK_LEXIS_NEXIS_5"')
    
    t6 = BashOperator(
        task_id='T6_LOAD_LEXIS_NEXIS_6',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 6 --landingtablename "WORK_LEXIS_NEXIS_6"')
    
    t7 = BashOperator(
        task_id='T7_LOAD_LEXIS_NEXIS_7',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 7 --landingtablename "WORK_LEXIS_NEXIS_7"')
    
    t8 = BashOperator(
        task_id='T8_LOAD_LEXIS_NEXIS_8',
        bash_command='python /home/airflow/gcs/data/GCPDWH/lexisnexis/load_lexisnexis_to_bigquery_dataflow.py --config "config.properties" --productconfig "lexisnexis.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 8 --landingtablename "WORK_LEXIS_NEXIS_8"')
    
 
    t_initial>>t0,
    t_initial>>t1,
    t_initial>>t2,
    t_initial>>t3,
    t_initial>>t4,
    t_initial>>t5,
    t_initial>>t6,
    t_initial>>t7,
    t_initial>>t8   
    
    