import time
import pprint

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 10),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

# utc time = pacific time + 8 hours
schedule_interval = "0 10 * * *"

pp = pprint.PrettyPrinter(indent=4)


def trigger_dag(context, dag_run_obj):
    dag_run_obj.payload = {'message': context['params']['message']}
    pp.pprint(dag_run_obj.payload)
    return dag_run_obj


with DAG('AMS_LANDING_1_to_30_TABLES_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    t1 = BashOperator(
        task_id='AMS_LANDING_1_to_30_TABLES_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ams/DATAFLOW_AMS_LANDING_1_to_30_TABLES.py '
                     '--config "config.properties" '
                     '--productconfig "ams.properties" '
                     '--env "{{var.value.environment}}" '
                     '--connectionprefix "ams" '
                     '--incrementaldate "1" '
    )

    trigger_dag = TriggerDagRunOperator(
        task_id='TRIGGER_AMS_OPERATIONAL_1_to_30_TABLES',
        trigger_dag_id='AMS_OPERATIONAL_1_to_30_TABLES',  # Ensure this equals the dag_id of the DAG to trigger
        python_callable=trigger_dag,
        params={'message': 'Triggering the DAG'},
        dag=dag
    )

# t1
t1 >> trigger_dag
