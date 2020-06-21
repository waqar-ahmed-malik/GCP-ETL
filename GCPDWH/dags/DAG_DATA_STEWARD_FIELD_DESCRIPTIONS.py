from airflow import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import logging

service_key = Variable.get("key_file")
logging.warning(service_key)

def load_data(**kwargs):
    client = bigquery.Client.from_service_account_json(service_key)
    logging.warning("starting")
    #Retrieves the list of tables derived from the prior step
    table_query = ("SELECT DISTINCT table_catalog, table_schema, table_name FROM LANDING.staged_descriptions")
    #Start the BQ job
    table_job = client.query(table_query,location="US",)
    #For each table returned
    for row in table_job:
        table_id = '{}.{}.{}'.format(row.table_catalog, row.table_schema, row.table_name)
        print(table_id)
        table = client.get_table(table_id)
        new_schema = []
        # Retrieves the fields for the current table with the new descriptions to be added
        field_query = (
            "SELECT name, type, mode, description FROM LANDING.staged_descriptions WHERE table_schema = '{}' and table_name = '{}'".format(row.table_schema, row.table_name))
        #Starts the subquery
        field_job = client.query(field_query, location="US", )
        #Adds each field to the new schema object. All fields from the originating table must be included. Fields which are in the
        #stewards google sheet but are not currently in the table will be ignored. data types and mode are also ignored
        for field_row in field_job:
            new_schema.append(bigquery.SchemaField(field_row.name, field_row.type, field_row.mode, field_row.description))
        #Applys the new schema object to the table object
        table.schema = new_schema
        #Patches the table with the new schema
        table = client.update_table(table, ["schema"])

        print(("Patched Table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 16),
    'email': ['dlitgcpdwsupport@norcal.aaa.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    'execution_timeout': timedelta(minutes=360)
}

dag = DAG('DAG_DATA_STEWARD_FIELD_DESCRIPTIONS',
          default_args=default_args,
          description='Field descriptions for tables in the CUSTOMER_PRODUCT, CUSTOMERS, OPERATIONAL, COR_ANALYTICS and REFERENCE datasets',
          schedule_interval='@once',
          template_searchpath='/home/airflow/gcs/data/GCPDWH/datastewards/sql')

staged_descriptions = BigQueryOperator(
    task_id='staged_descriptions',
    bql='/temp_staged_descriptions.sql',
    destination_dataset_table='{{var.value.project}}.LANDING.staged_descriptions',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='aaa_bq_conn',
    dag=dag)

patch_table = PythonOperator(python_callable=load_data, task_id='patch_table', dag=dag)

staged_descriptions >> patch_table