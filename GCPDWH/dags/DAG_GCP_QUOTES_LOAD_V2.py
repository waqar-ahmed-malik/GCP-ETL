from datetime import datetime, timedelta
from airflow import DAG


from airflow.operators.email_operator import EmailOperator


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 0o5, 0o1),
    'email': ['william.moore@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360)
}

with DAG('DAG_GCP_QUOTES_MAIL_TEST', schedule_interval=None, catchup=False, default_args=default_dag_args,) as dag:

    t1 = EmailOperator(
        task_id='email_file_absence',
        to='prerna.anand@norcal.aaa.com',
        subject='PAS QSR file not present',
        files=['gs://dw-prod-pcomp/count.txt'],
        html_content="""
        File not present for today.
        """)
t1