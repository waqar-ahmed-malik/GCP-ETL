import airflow
import logging

from airflow import DAG
from google.cloud import storage
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
# from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator

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

logging.info(Variable.get("environment"))

date_time_stamp = str(datetime.now()).replace('-', '').replace(' ', '').replace(':', '')[:14]

with DAG('AMS_OPERATIONAL_1_to_30_TABLES', schedule_interval=None, catchup=False,
         default_args=default_args, template_searchpath='/home/airflow/gcs/data/GCPDWH/ams/sql') as dag:

    t1 = BigQueryOperator(
        task_id='AMS_BRANCH',
        bql='/OPERATIONAL_AMS_BRANCH.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_BRANCH',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t2 = BigQueryOperator(
        task_id='AMS_CD_POLICY_LINE_TYPE',
        bql='/OPERATIONAL_AMS_CD_POLICY_LINE_TYPE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CD_POLICY_LINE_TYPE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t3 = BigQueryOperator(
        task_id='AMS_CLIENT',
        bql='/OPERATIONAL_AMS_CLIENT.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CLIENT',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t4 = BigQueryOperator(
        task_id='AMS_CLIENT_AGENCY_BRANCH_JT',
        bql='/OPERATIONAL_AMS_CLIENT_AGENCY_BRANCH_JT.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CLIENT_AGENCY_BRANCH_JT',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t5 = BigQueryOperator(
        task_id='AMS_COMPANY',
        bql='/OPERATIONAL_AMS_COMPANY.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_COMPANY',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t6 = BigQueryOperator(
        task_id='AMS_CONFIGURE_LK_LANGUAGE_RESOURCE',
        bql='/OPERATIONAL_AMS_CONFIGURE_LK_LANGUAGE_RESOURCE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CONFIGURE_LK_LANGUAGE_RESOURCE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

#     t7 = BigQueryOperator(
#         task_id='AMS_CONTACT_ADDRESS',
#         bql='/OPERATIONAL_AMS_CONTACT_ADDRESS.sql',
#         # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CONTACT_ADDRESS',
#         use_legacy_sql=False,
#         write_disposition='WRITE_APPEND',
#         create_disposition='CREATE_IF_NEEDED',
#         bigquery_conn_id='aaa_bq_conn')

#     t8 = BigQueryOperator(
#         task_id='AMS_CONTACT_CLASS',
#         bql='/OPERATIONAL_AMS_CONTACT_CLASS.sql',
#         # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CONTACT_CLASS',
#         use_legacy_sql=False,
#         write_disposition='WRITE_APPEND',
#         create_disposition='CREATE_IF_NEEDED',
#         bigquery_conn_id='aaa_bq_conn')

#     t9 = BigQueryOperator(
#         task_id='AMS_CONTACT_NAME',
#         bql='/OPERATIONAL_AMS_CONTACT_NAME.sql',
#         # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CONTACT_NAME',
#         use_legacy_sql=False,
#         write_disposition='WRITE_APPEND',
#         create_disposition='CREATE_IF_NEEDED',
#         bigquery_conn_id='aaa_bq_conn')

#     t10 = BigQueryOperator(
#         task_id='AMS_CONTACT_NUMBER',
#         bql='/OPERATIONAL_AMS_CONTACT_NUMBER.sql',
#         # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_CONTACT_NUMBER',
#         use_legacy_sql=False,
#         write_disposition='WRITE_APPEND',
#         create_disposition='CREATE_IF_NEEDED',
#         bigquery_conn_id='aaa_bq_conn')

    t11 = BigQueryOperator(
        task_id='AMS_COVERAGE',
        bql='/OPERATIONAL_AMS_COVERAGE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_COVERAGE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t12 = BigQueryOperator(
        task_id='AMS_EMPLOYEE',
        bql='/OPERATIONAL_AMS_EMPLOYEE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_EMPLOYEE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t13 = BigQueryOperator(
        task_id='AMS_ENTITY_EMPLOYEE_JT',
        bql='/OPERATIONAL_AMS_ENTITY_EMPLOYEE_JT.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_ENTITY_EMPLOYEE_JT',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t14 = BigQueryOperator(
        task_id='AMS_LINE',
        bql='/OPERATIONAL_AMS_LINE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_LINE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t15 = BigQueryOperator(
        task_id='AMS_LINE_BROKER_PRODUCER',
        bql='/OPERATIONAL_AMS_LINE_BROKER_PRODUCER.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_LINE_BROKER_PRODUCER',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t16 = BigQueryOperator(
        task_id='AMS_LINE_IMAGE',
        bql='/OPERATIONAL_AMS_LINE_IMAGE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_LINE_IMAGE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t17 = BigQueryOperator(
        task_id='AMS_LOCATION_COVERAGE',
        bql='/OPERATIONAL_AMS_LOCATION_COVERAGE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_LOCATION_COVERAGE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t18 = BigQueryOperator(
        task_id='AMS_PA_ADDITIONAL_COVERAGE',
        bql='/OPERATIONAL_AMS_PA_ADDITIONAL_COVERAGE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_PA_ADDITIONAL_COVERAGE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t19 = BigQueryOperator(
        task_id='AMS_PA_DRIVER',
        bql='/OPERATIONAL_AMS_PA_DRIVER.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_PA_DRIVER',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t20 = BigQueryOperator(
        task_id='AMS_PA_IMAGE',
        bql='/OPERATIONAL_AMS_PA_IMAGE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_PA_IMAGE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t21 = BigQueryOperator(
        task_id='AMS_POLICY',
        bql='/OPERATIONAL_AMS_POLICY.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_POLICY',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t22 = BigQueryOperator(
        task_id='AMS_RS_ADDITIONAL_INTEREST',
        bql='/OPERATIONAL_AMS_RS_ADDITIONAL_INTEREST.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_RS_ADDITIONAL_INTEREST',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t23 = BigQueryOperator(
        task_id='AMS_RS_IMAGE',
        bql='/OPERATIONAL_AMS_RS_IMAGE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_RS_IMAGE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t24 = BigQueryOperator(
        task_id='AMS_SECURITY_USER',
        bql='/OPERATIONAL_AMS_SECURITY_USER.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_SECURITY_USER',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t25 = BigQueryOperator(
        task_id='AMS_SECURITY_USER_STRUCTURE_COMBINATION_JT',
        bql='/OPERATIONAL_AMS_SECURITY_USER_STRUCTURE_COMBINATION_JT.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_SECURITY_USER_STRUCTURE_COMBINATION_JT',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t26 = BigQueryOperator(
        task_id='AMS_STRUCTURE_COMBINATION',
        bql='/OPERATIONAL_AMS_STRUCTURE_COMBINATION.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_STRUCTURE_COMBINATION',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t27 = BigQueryOperator(
        task_id='AMS_TRANS_CODE',
        bql='/OPERATIONAL_AMS_TRANS_CODE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_TRANS_CODE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t28 = BigQueryOperator(
        task_id='AMS_TRANS_DETAIL',
        bql='/OPERATIONAL_AMS_TRANS_DETAIL.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_TRANS_DETAIL',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t29 = BigQueryOperator(
        task_id='AMS_TRANS_HEAD',
        bql='/OPERATIONAL_AMS_TRANS_HEAD.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_TRANS_HEAD',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

    t30 = BigQueryOperator(
        task_id='AMS_VEHICLE_COVERAGE',
        bql='/OPERATIONAL_AMS_VEHICLE_COVERAGE.sql',
        # destination_dataset_table='{{var.value.project}}.OPERATIONAL.AMS_VEHICLE_COVERAGE',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='aaa_bq_conn')

[
    t1,
    t2,
    t3,
    t4,
    t5,
    t6,
#     t7,
#     t8,
#     t9,
#     t10,
    t11,
    t12,
    t13,
    t14,
    t15,
    t16,
    t17,
    t18,
    t19,
    t20,
    t21,
    t22,
    t23,
    t24,
    t25,
    t26,
    t27,
    t28,
    t29,
    t30
]

