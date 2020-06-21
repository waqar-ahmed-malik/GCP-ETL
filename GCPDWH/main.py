import pytds
import pandas
import os
from google.cloud import bigquery


def migrate(request):
    client = bigquery.Client()
    with pytds.connect(os.environ['SQL_SERVER_HOST'], os.environ['SQL_SERVER_DATABASE'], os.environ['SQL_SERVER_USERNAME'], os.environ['SQL_SERVER_PASSWORD']) as conn:
        query_job = client.query(f"SELECT * FROM `{os.environ['BQ_CONFIG_DATASET']}.{os.environ['BQ_CONFIG_TABLE']}` WHERE Function_ID = {os.environ['FUNCTION_ID']} AND Migration_Frequency = '{os.environ['MIGRATION_FREQUENCY']}'")
        results = query_job.result()
        for row in results:
            i = 0
            try:
                query = f"SELECT * FROM {row['SQL_Server_Schema']}.{row['SQL_Server_Table']}"
                for chunk in pandas.read_sql(sql=query, con=conn, coerce_float=False, chunksize=1000):
                    chunk.columns = chunk.columns.str.replace('%', '_Percentage')
                    if i == 0:
                        chunk.to_gbq(destination_table=f"{row['BQ_Dataset']}.{row['BQ_Table']}", project_id=os.environ['BQ_PROJECT_ID'], if_exists='replace')
                    else:
                        chunk.to_gbq(destination_table=f"{row['BQ_Dataset']}.{row['BQ_Table']}", project_id=os.environ['BQ_PROJECT_ID'], if_exists='append')
                    i = i + 1
            except Exception as e:
                print(f"Error in migrating {row['SQL_Server_Table']}")
                print(e)
            else:
                continue