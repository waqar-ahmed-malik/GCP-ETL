from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# local_tz = pendulum.timezone("US/Pacific")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 26, 13, 0, 0), #tzinfo=local_tz),
    'email': ['dlitgcpalerts@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG('DAG_CALL_VOLUME', schedule_interval='00 16 * * *', catchup=False, default_args=default_args) as dag:
    t0 = BashOperator(
        task_id='AUTH_ACCOUNT',
        bash_command='gcloud auth activate-service-account ers-prod-svc-ml-codepush@aaa-mwg-ersprod.iam.gserviceaccount.com '
                     '--key-file=/home/airflow/gcs/data/jsonkeys/ml_test_service_account.json')

    t1 = BashOperator(
        task_id='SET_PROJECT',
        bash_command='gcloud config set project aaa-mwg-ersprod')

    t2 = BashOperator(
        task_id='CALL_VOLUME',
        bash_command='python /home/airflow/gcs/data/ERSCALLVOLUME/call_volume_dataflow.py '
                     '--runner=DataflowRunner --project=aaa-mwg-ersprod '
                     '--temp_location=gs://ers_ml_prod/call_volume_full_footprint/tmp '
                     '--staging_location=gs://ers_ml_prod/call_volume_full_footprint/stg '
                     '--setup_file=/home/airflow/gcs/data/ERSCALLVOLUME/setup.py ')

    # Define the DAG structure.
    t0 >> t1 >> t2
