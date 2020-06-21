from airflow import DAG

from datetime import datetime, timedelta

from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.bash_operator import BashOperator

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator



worker_config = Variable.get("workday_worker", deserialize_json=True)

worker_schema = worker_config["input_fields"]



sup_hier_config = Variable.get("workday_supervisory_hierarchy", deserialize_json=True)

sup_hier_schema = sup_hier_config["input_fields"]



mkt_hier_config = Variable.get("workday_market_hierarchy", deserialize_json=True)

mkt_hier_schema = mkt_hier_config["input_fields"]



location_config = Variable.get("workday_locations", deserialize_json=True)

location_schema = location_config["input_fields"]



job_codes_config = Variable.get("workday_job_codes", deserialize_json=True)

job_codes_schema = job_codes_config["input_fields"]



costcenter_config = Variable.get("workday_costcenter", deserialize_json=True)

costcenter_schema = costcenter_config["input_fields"]



default_args = {

    'owner': 'airflow',

    'depends_on_past': False,

    'start_date': datetime(2020, 1, 13),

    'email': ['dlitgcpdwsupport@norcal.aaa.com'],  # ['dlitgcpdwsupport@norcal.aaa.com'],

    'email_on_success': True,

    'email_on_failure': True,

    'email_on_retry': False,

    'retries': 1,

    'retry_delay': timedelta(minutes=2),

    'execution_timeout': timedelta(minutes=360)

}



schedule_interval = '00 07 * * *'

# Temp schedule interval for testing

#schedule_interval = timedelta(hours=24)



# Note the template search path. Airflow, by default searches the DAG directory for query templates. You'll need to tell it otherwise

with DAG('DAG-GCP-WD-LOAD-PIPELINE', schedule_interval=schedule_interval, catchup=True, default_args=default_args,

         template_searchpath='/home/airflow/gcs/data/GCPDWH/workday/sql') as dag:

    first_task = BashOperator(

        task_id='first_task',

        bash_command='echo test')

    # Raw tables are used to isolate file issues from the daily history tables. 

    raw_worker = GoogleCloudStorageToBigQueryOperator(

        task_id='load_worker_file_to_raw_table',

        bucket='{{ var.json.workday_worker.bucket }}',

        source_objects=[

            '{{ var.json.workday_worker.directory }}/{{ var.json.workday_worker.filename_prefix }}.{{ execution_date.strftime("%m.%d.%Y") }}{{ var.json.workday_worker.filename_suffix }}'

        ],

        destination_project_dataset_table='{{ var.json.workday_worker.target_project }}.{{ var.json.workday_worker.target_dataset }}.{{ var.json.workday_worker.target_table }}{{ ds_nodash }}',

        source_format='CSV',

        schema_fields=worker_schema,

        create_disposition='CREATE_IF_NEEDED',

        skip_leading_rows=1,

        write_disposition='WRITE_TRUNCATE',

        field_delimiter=',',

        quote_character='"',

        max_bad_records=100,

        ignore_unknown_values=False,

        google_cloud_storage_conn_id='aaa_bq_conn',

        bigquery_conn_id='aaa_bq_conn',

        autodetect=False)

    # Raw tables are used to isolate file issues from the daily history tables. 

    raw_sup_hier = GoogleCloudStorageToBigQueryOperator(

        task_id='load_supervisory_hierarchy_file_to_raw_table',

        bucket='{{ var.json.workday_supervisory_hierarchy.bucket }}',

        source_objects=[

            '{{ var.json.workday_supervisory_hierarchy.directory }}/{{ var.json.workday_supervisory_hierarchy.filename_prefix }}.{{ execution_date.strftime("%m.%d.%Y") }}{{ var.json.workday_supervisory_hierarchy.filename_suffix }}'

        ],

        destination_project_dataset_table='{{ var.json.workday_supervisory_hierarchy.target_project }}.{{ var.json.workday_supervisory_hierarchy.target_dataset }}.{{ var.json.workday_supervisory_hierarchy.target_table }}{{ ds_nodash }}',

        source_format='CSV',

        schema_fields=sup_hier_schema,

        create_disposition='CREATE_IF_NEEDED',

        skip_leading_rows=1,

        write_disposition='WRITE_TRUNCATE',

        field_delimiter=',',

        quote_character='"',

        max_bad_records=100,

        ignore_unknown_values=False,

        google_cloud_storage_conn_id='aaa_prod_bq_conn',

        bigquery_conn_id='aaa_bq_conn',

        autodetect=True)

    # Raw tables are used to isolate file issues from the daily history tables. 

    raw_mkt_hier = GoogleCloudStorageToBigQueryOperator(

        task_id='load_market_hierarchy_file_to_raw_table',

        bucket='{{ var.json.workday_market_hierarchy.bucket }}',

        source_objects=[

            '{{ var.json.workday_market_hierarchy.directory }}/{{ var.json.workday_market_hierarchy.filename_prefix }}.{{ execution_date.strftime("%m.%d.%Y") }}{{ var.json.workday_market_hierarchy.filename_suffix }}'

        ],

        destination_project_dataset_table='{{ var.json.workday_market_hierarchy.target_project }}.{{ var.json.workday_market_hierarchy.target_dataset }}.{{ var.json.workday_market_hierarchy.target_table }}{{ ds_nodash }}',

        source_format='CSV',

        schema_fields=mkt_hier_schema,

        create_disposition='CREATE_IF_NEEDED',

        skip_leading_rows=1,

        write_disposition='WRITE_TRUNCATE',

        field_delimiter=',',

        quote_character='"',

        max_bad_records=100,

        ignore_unknown_values=False,

        google_cloud_storage_conn_id='aaa_bq_conn',

        bigquery_conn_id='aaa_bq_conn',

        autodetect=True)

    # Raw tables are used to isolate file issues from the daily history tables. 

    raw_location = GoogleCloudStorageToBigQueryOperator(

        task_id='load_location_file_to_raw_table',

        bucket='{{ var.json.workday_locations.bucket }}',

        source_objects=[

            '{{ var.json.workday_locations.directory }}/{{ var.json.workday_locations.filename_prefix }}.{{ execution_date.strftime("%m.%d.%Y") }}{{ var.json.workday_locations.filename_suffix }}'

        ],

        destination_project_dataset_table='{{ var.json.workday_locations.target_project }}.{{ var.json.workday_locations.target_dataset }}.{{ var.json.workday_locations.target_table }}{{ ds_nodash }}',

        source_format='CSV',

        schema_fields=location_schema,

        create_disposition='CREATE_IF_NEEDED',

        skip_leading_rows=1,

        write_disposition='WRITE_TRUNCATE',

        field_delimiter=',',

        quote_character='"',

        max_bad_records=100,

        ignore_unknown_values=False,

        google_cloud_storage_conn_id='aaa_bq_conn',

        bigquery_conn_id='aaa_bq_conn',

        autodetect=True)

    # Raw tables are used to isolate file issues from the daily history tables. 

    raw_job_codes = GoogleCloudStorageToBigQueryOperator(

        task_id='load_job_codes_file_to_raw_table',

        bucket='{{ var.json.workday_job_codes.bucket }}',

        source_objects=[

            '{{ var.json.workday_job_codes.directory }}/{{ var.json.workday_job_codes.filename_prefix }}.{{ execution_date.strftime("%m.%d.%Y") }}{{ var.json.workday_job_codes.filename_suffix }}'

        ],

        destination_project_dataset_table='{{ var.json.workday_job_codes.target_project }}.{{ var.json.workday_job_codes.target_dataset }}.{{ var.json.workday_job_codes.target_table }}{{ ds_nodash }}',

        source_format='CSV',

        schema_fields=job_codes_schema,

        create_disposition='CREATE_IF_NEEDED',

        skip_leading_rows=1,

        write_disposition='WRITE_TRUNCATE',

        field_delimiter=',',

        quote_character='"',

        max_bad_records=100,

        ignore_unknown_values=False,

        google_cloud_storage_conn_id='aaa_bq_conn',

        bigquery_conn_id='aaa_bq_conn',

        autodetect=True)

    # Raw tables are used to isolate file issues from the daily history tables. 

    raw_costcenter = GoogleCloudStorageToBigQueryOperator(

        task_id='load_costcenter_file_to_raw_table',

        bucket='{{ var.json.workday_costcenter.bucket }}',

        source_objects=[

            '{{ var.json.workday_costcenter.directory }}/{{ var.json.workday_costcenter.filename_prefix }}.{{ execution_date.strftime("%m.%d.%Y") }}{{ var.json.workday_costcenter.filename_suffix }}'

        ],

        destination_project_dataset_table='{{ var.json.workday_costcenter.target_project }}.{{ var.json.workday_costcenter.target_dataset }}.{{ var.json.workday_costcenter.target_table }}{{ ds_nodash }}',

        source_format='CSV',

        schema_fields=costcenter_schema,

        create_disposition='CREATE_IF_NEEDED',

        skip_leading_rows=1,

        write_disposition='WRITE_TRUNCATE',

        field_delimiter=',',

        quote_character='"',

        max_bad_records=100,

        ignore_unknown_values=False,

        google_cloud_storage_conn_id='aaa_bq_conn',

        bigquery_conn_id='aaa_bq_conn',

        autodetect=True)



    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.

    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  

    delete_worker_daily_history = BigQueryOperator(

        task_id='delete_from_worker_daily_history',

        bql='DELETE FROM LANDING.WD_WORKER_DAILY_HISTORY WHERE fileid = {{ ds_nodash }}',

        use_legacy_sql=False,

        bigquery_conn_id='aaa_bq_conn')



    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.

    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  

    delete_costcenter_daily_history = BigQueryOperator(

        task_id='delete_from_costcenter_daily_history',

        bql='DELETE FROM LANDING.WD_COSTCENTER_DAILY_HISTORY WHERE fileid = {{ ds_nodash }}',

        use_legacy_sql=False,

        bigquery_conn_id='aaa_bq_conn')



    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.

    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  

    delete_job_codes_daily_history = BigQueryOperator(

        task_id='delete_from_job_codes_daily_history',

        bql='DELETE FROM LANDING.WD_JOB_CODES_DAILY_HISTORY WHERE fileid = {{ ds_nodash }}',

        use_legacy_sql=False,

        bigquery_conn_id='aaa_bq_conn')



    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.

    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  

    delete_location_daily_history = BigQueryOperator(

        task_id='delete_from_location_daily_history',

        bql='DELETE FROM LANDING.WD_LOCATION_DAILY_HISTORY WHERE fileid = {{ ds_nodash }}',

        use_legacy_sql=False,

        bigquery_conn_id='aaa_bq_conn')



    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.

    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  

    delete_market_hierarchy_daily_history = BigQueryOperator(

        task_id='delete_from_market_hierarchy_daily_history',

        bql='DELETE FROM LANDING.WD_MARKET_HIERARCHY_DAILY_HISTORY WHERE fileid = {{ ds_nodash }}',

        use_legacy_sql=False,

        bigquery_conn_id='aaa_bq_conn')



    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.

    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  

    delete_supervisory_hierarchy_daily_history = BigQueryOperator(

        task_id='delete_from_supervisory_hierarchy_daily_history',

        bql='DELETE FROM LANDING.WD_SUPERVISORY_HIERARCHY_DAILY_HISTORY WHERE fileid = {{ ds_nodash }}',

        use_legacy_sql=False,

        bigquery_conn_id='aaa_bq_conn')



    # Since the daily history tables are append only. The delete tasks will clear out records for a given execution date.

    # This is better (Fast/Cheaper) than running a merge statement. If no records exist for a date, it does nothing  

    worker_daily_history = BigQueryOperator(

        task_id='raw_table_to_worker_daily_history',

        bql='/worker_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_WORKER_DAILY_HISTORY',

        use_legacy_sql=False,

        write_disposition='WRITE_APPEND',

        bigquery_conn_id='aaa_bq_conn')



    p_c_3_digit_cleanup_pt1 = BigQueryOperator(

        task_id='pc_3_digit_cleanup_pt1',

        bql='/operational_worker_p_c_3_digit_cleanup_pt1.sql',

        destination_dataset_table='{{var.value.project}}.OPERATIONAL.P_C_3_part1',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        bigquery_conn_id='aaa_bq_conn')

        

    p_c_3_digit_cleanup = BigQueryOperator(

        task_id='pc_3_digit_cleanup',

        bql='/operational_worker_p_c_3_digit_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.OPERATIONAL.Worker_P_C_3_Digit_cleanup',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        bigquery_conn_id='aaa_bq_conn')



    travel_id_cleanup_pt1 = BigQueryOperator(

        task_id='travel_id_cleanup_pt1',

        bql='/operational_worker_travel_id_cleanup_pt1.sql',

        destination_dataset_table='{{var.value.project}}.OPERATIONAL.TRAVEL_ID_part1',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        bigquery_conn_id='aaa_bq_conn')



    travel_id_cleanup = BigQueryOperator(

        task_id='travel_id_cleanup',

        bql='/operational_worker_travel_id_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.OPERATIONAL.Worker_Travel_ID_cleanup',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        bigquery_conn_id='aaa_bq_conn')



    termination_date_cleanup = BigQueryOperator(

        task_id='termination_date_cleanup',

        bql='/operational_worker_effective_termination_date_banding.sql',

        destination_dataset_table='{{var.value.project}}.OPERATIONAL.Worker_effective_termination_date_banding',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        bigquery_conn_id='aaa_bq_conn')



    hire_date_cleanup = BigQueryOperator(

        task_id='hire_date_cleanup',

        bql='/operational_worker_effective_hire_date_banding.sql',

        destination_dataset_table='{{var.value.project}}.OPERATIONAL.Worker_effective_hire_date_banding',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        bigquery_conn_id='aaa_bq_conn')



    finish_cleanup = DummyOperator(task_id='finish_cleanup')



    costcenter_daily_history = BigQueryOperator(

        task_id='raw_table_to_costcenter_daily_history',

        bql='/costcenter_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_COSTCENTER_DAILY_HISTORY',

        use_legacy_sql=False,

        write_disposition='WRITE_APPEND',

        bigquery_conn_id='aaa_bq_conn')



    job_codes_daily_history = BigQueryOperator(

        task_id='raw_table_to_job_codes_daily_history',

        bql='/job_codes_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_JOB_CODES_DAILY_HISTORY',

        use_legacy_sql=False,

        write_disposition='WRITE_APPEND',

        bigquery_conn_id='aaa_bq_conn')



    location_daily_history = BigQueryOperator(

        task_id='raw_table_to_location_daily_history',

        bql='/location_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_LOCATION_DAILY_HISTORY',

        use_legacy_sql=False,

        write_disposition='WRITE_APPEND',

        bigquery_conn_id='aaa_bq_conn')



    market_hierarchy_daily_history = BigQueryOperator(

        task_id='raw_table_to_market_hierarchy_daily_history',

        bql='/market_hierarchy_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_MARKET_HIERARCHY_DAILY_HISTORY',

        use_legacy_sql=False,

        write_disposition='WRITE_APPEND',

        bigquery_conn_id='aaa_bq_conn')



    supervisory_hierarchy_daily_history = BigQueryOperator(

        task_id='raw_table_to_supervisory_hierarchy_daily_history',

        bql='/supervisory_hierarchy_cleanup.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_SUPERVISORY_HIERARCHY_DAILY_HISTORY',

        use_legacy_sql=False,

        write_disposition='WRITE_APPEND',

        bigquery_conn_id='aaa_bq_conn')



    worker_dedupe_1a = BigQueryOperator(

        task_id='worker_dedupe_1a',

        bql='/worker_dedupe_1a.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_WORKER_DEDUPE_1a',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    worker_dedupe_1b = BigQueryOperator(

        task_id='worker_dedupe_1b',

        bql='/worker_dedupe_1b.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_WORKER_DEDUPE_1b',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    worker_dedupe_2a = BigQueryOperator(

        task_id='worker_dedupe_2a',

        bql='/worker_dedupe_2a.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_WORKER_DEDUPE_2a',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    worker_dedupe_2b = BigQueryOperator(

        task_id='worker_dedupe_2b',

        bql='/worker_dedupe_2b.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_WORKER_DEDUPE_2b',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    worker_dedupe_final = BigQueryOperator(

        task_id='worker_dedupe_final',

        bql='/worker_dedupe_final.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_WORKER',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    costcenter_dedupe_final = BigQueryOperator(

        task_id='costcenter_dedupe_final',

        bql='/costcenter_dedupe_final.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_COSTCENTER',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    market_hierarchy_dedupe_final = BigQueryOperator(

        task_id='market_hierarchy_dedupe_final',

        bql='/market_hierarchy_dedupe_final.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_MARKET_HIERARCHY',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    location_dedupe_final = BigQueryOperator(

        task_id='location_dedupe_final',

        bql='/location_dedupe_final.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_LOCATION',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    job_codes_dedupe_final = BigQueryOperator(

        task_id='job_codes_dedupe_final',

        bql='/job_codes_dedupe_final.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_JOB_CODES',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    supervisory_hierarchy_dedupe_final = BigQueryOperator(

        task_id='supervisory_hierarchy_dedupe_final',

        bql='/supervisory_hierarchy_dedupe_final.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.WD_SUPERVISORY_HIERARCHY',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    landing_location_combined_pt1 = BigQueryOperator(

        task_id='landing_location_combined_pt1',

        bql='/landing_location_combined_pt1.sql',

        destination_dataset_table='{{var.value.project}}.LANDING.LOCATION_COMBINED_PT1',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')

        

    placeholder = DummyOperator(task_id='placeholder')



    customer_product_employee_dim = BigQueryOperator(

        task_id='customer_product_employee_dim',

        bql='/customer_product_employee_dim.sql',

        destination_dataset_table='{{var.value.project}}.CUSTOMER_PRODUCT.EMPLOYEE_DIM',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    customers_employee_profile_dim = BigQueryOperator(

        task_id='customers_employee_profile_dim',

        bql='/customers_employee_profile_dim.sql',

        destination_dataset_table='{{var.value.project}}.CUSTOMERS.EMPLOYEE_PROFILE_DIM',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    customer_product_location_dim = BigQueryOperator(

        task_id='customer_product_location_dim',

        bql='/customer_product_location_dim2.sql',

        destination_dataset_table='{{var.value.project}}.CUSTOMER_PRODUCT.LOCATION_DIM',

        use_legacy_sql=False,

        write_disposition='WRITE_TRUNCATE',

        create_disposition='CREATE_IF_NEEDED',

        bigquery_conn_id='aaa_bq_conn')



    first_task.set_downstream(raw_worker)

    first_task.set_downstream(raw_sup_hier)

    first_task.set_downstream(raw_mkt_hier)

    first_task.set_downstream(raw_location)

    first_task.set_downstream(raw_job_codes)

    first_task.set_downstream(raw_costcenter)



    raw_worker.set_downstream(delete_worker_daily_history)

    raw_sup_hier.set_downstream(delete_supervisory_hierarchy_daily_history)

    raw_mkt_hier.set_downstream(delete_market_hierarchy_daily_history)

    raw_location.set_downstream(delete_location_daily_history)

    raw_job_codes.set_downstream(delete_job_codes_daily_history)

    raw_costcenter.set_downstream(delete_costcenter_daily_history)



    delete_worker_daily_history.set_downstream(worker_daily_history)

    delete_supervisory_hierarchy_daily_history.set_downstream(supervisory_hierarchy_daily_history)

    delete_market_hierarchy_daily_history.set_downstream(market_hierarchy_daily_history)

    delete_location_daily_history.set_downstream(location_daily_history)

    delete_job_codes_daily_history.set_downstream(job_codes_daily_history)

    delete_costcenter_daily_history.set_downstream(costcenter_daily_history)



    worker_daily_history.set_downstream(p_c_3_digit_cleanup_pt1)

    p_c_3_digit_cleanup_pt1.set_downstream(p_c_3_digit_cleanup)



    worker_daily_history.set_downstream(travel_id_cleanup_pt1)

    travel_id_cleanup_pt1.set_downstream(travel_id_cleanup)



    worker_daily_history.set_downstream(termination_date_cleanup)

    worker_daily_history.set_downstream(hire_date_cleanup)



    p_c_3_digit_cleanup.set_downstream(finish_cleanup)

    travel_id_cleanup.set_downstream(finish_cleanup)

    termination_date_cleanup.set_downstream(finish_cleanup)

    hire_date_cleanup.set_downstream(finish_cleanup)



    finish_cleanup.set_downstream(worker_dedupe_1a)

    finish_cleanup.set_downstream(worker_dedupe_1b)



    worker_dedupe_1a.set_downstream(worker_dedupe_2a)

    worker_dedupe_1b.set_downstream(worker_dedupe_2b)



    worker_dedupe_2a.set_downstream(worker_dedupe_final)

    worker_dedupe_2b.set_downstream(worker_dedupe_final)



    supervisory_hierarchy_daily_history.set_downstream(supervisory_hierarchy_dedupe_final)

    market_hierarchy_daily_history.set_downstream(market_hierarchy_dedupe_final)

    location_daily_history.set_downstream(location_dedupe_final)

    job_codes_daily_history.set_downstream(job_codes_dedupe_final)

    costcenter_daily_history.set_downstream(costcenter_dedupe_final)



    location_daily_history.set_downstream(landing_location_combined_pt1)

    market_hierarchy_daily_history.set_downstream(landing_location_combined_pt1)



    worker_dedupe_final.set_downstream(placeholder)

    supervisory_hierarchy_dedupe_final.set_downstream(placeholder)

    market_hierarchy_dedupe_final.set_downstream(placeholder)

    location_dedupe_final.set_downstream(placeholder)

    job_codes_dedupe_final.set_downstream(placeholder)

    costcenter_dedupe_final.set_downstream(placeholder)

    landing_location_combined_pt1.set_downstream(placeholder)



    placeholder.set_downstream(customer_product_employee_dim)

    placeholder.set_downstream(customers_employee_profile_dim)

    placeholder.set_downstream(customer_product_location_dim)
