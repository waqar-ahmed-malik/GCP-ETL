from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o7, 16),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval='30 17 9 * *'

with DAG('DAG_GCP_DIRECT_FOCUS_INDIVIDUAL_MONTHLY_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t_initial = BashOperator(
        task_id='T_initial_DELETE_DIRECTFOCUS_INDIVIDUAL_DATA',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/delete_directfocus_individual_monthly_work_tables.py --config "config.properties" --productconfig "directfocus.properties" --env {{ var.value.environment }} --sqlfile "/sql/Monthly/delete_directfocus_monthly_data.sql"')
    
    t0 = BashOperator(
        task_id='T0_LOAD_DIRECTFOCUS_INDIVIDUAL_0',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_individual_monthly_work_tables_dataflow.py --config "config.properties" --productconfig "directfocus.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 0 --landingtablename "WORK_INDIVIDUAL_0"')
    
    t1 = BashOperator(
        task_id='T1_LOAD_DIRECTFOCUS_INDIVIDUAL_1',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_individual_monthly_work_tables_dataflow.py --config "config.properties" --productconfig "directfocus.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 1 --landingtablename "WORK_INDIVIDUAL_1"')
    
    t2 = BashOperator(
        task_id='T2_LOAD_DIRECTFOCUS_INDIVIDUAL_2',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_individual_monthly_work_tables_dataflow.py --config "config.properties" --productconfig "directfocus.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 2 --landingtablename "WORK_INDIVIDUAL_2"')
    
    t3 = BashOperator(
        task_id='T3_LOAD_DIRECTFOCUS_INDIVIDUAL_3',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_individual_monthly_work_tables_dataflow.py --config "config.properties" --productconfig "directfocus.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 3 --landingtablename "WORK_INDIVIDUAL_3"')
    
    t4 = BashOperator(
        task_id='T4_LOAD_DIRECTFOCUS_INDIVIDUAL_4',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_individual_monthly_work_tables_dataflow.py --config "config.properties" --productconfig "directfocus.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 4 --landingtablename "WORK_INDIVIDUAL_4"')
    
    t5 = BashOperator(
        task_id='T4_LOAD_DIRECTFOCUS_INDIVIDUAL_DIM',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_individual_dim_monthly_table_dataflow.py --config "config.properties" --productconfig "directfocus.properties" --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1')
 
    t_initial>>[t0,t1,t2,t3,t4]>>t5
