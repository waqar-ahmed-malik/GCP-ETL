from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 19),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

schedule_interval = '30 17 * * SUN'


with DAG('DAG_GCP_DIRECT_FOCUS_ENGAGE_JOURNEY_HISTORY_WEEKEND_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t0 = BashOperator(
        task_id='T0_DELETE_WORK_ENGAGE_JOURNEY_HISTORY_TABLES',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/delete_directfocus_individual_monthly_work_tables.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --sqlfile "/sql/Weekend/delete_work_df_engage_journey_history_weekend.sql"')
		   
    t1 = BashOperator(
        task_id='T1_LOAD_WORK_DF_ENGAGE_JOURNEY_HISTORY_0',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_engage_journey_history_weekend_work_tables_dataflow.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 0 --landingtablename "WORK_DF_ENGAGE_JOURNEY_HISTORY_0"')
    
    t2 = BashOperator(
        task_id='T2_LOAD_WORK_DF_ENGAGE_JOURNEY_HISTORY_1',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_engage_journey_history_weekend_work_tables_dataflow.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 1 --landingtablename "WORK_DF_ENGAGE_JOURNEY_HISTORY_1"')
    	
    t3 = BashOperator(
        task_id='T3_LOAD_WORK_DF_ENGAGE_JOURNEY_HISTORY_2',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_engage_journey_history_weekend_work_tables_dataflow.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 2 --landingtablename "WORK_DF_ENGAGE_JOURNEY_HISTORY_2"')

    t4 = BashOperator(
        task_id='T4_LOAD_WORK_DF_ENGAGE_JOURNEY_HISTORY_3',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_engage_journey_history_weekend_work_tables_dataflow.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 3 --landingtablename "WORK_DF_ENGAGE_JOURNEY_HISTORY_3"')

    t5 = BashOperator(
        task_id='T5_LOAD_WORK_DF_ENGAGE_JOURNEY_HISTORY_4',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_engage_journey_history_weekend_work_tables_dataflow.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 4 --landingtablename "WORK_DF_ENGAGE_JOURNEY_HISTORY_4"')

    t6 = BashOperator(
        task_id='T6_LOAD_WORK_DF_ENGAGE_JOURNEY_HISTORY_5',
        bash_command='python /home/airflow/gcs/data/GCPDWH/direct_focus/load_directfocus_engage_journey_history_weekend_work_tables_dataflow.py --config config.properties --productconfig directfocus.properties --env {{ var.value.environment }} --connectionprefix "df" --incrementaldate 1 --partition 5 --landingtablename "WORK_DF_ENGAGE_JOURNEY_HISTORY_5"')


    t0>>[t1,t2,t3,t4,t5,t6]
    
