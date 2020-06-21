import argparse
import apache_beam as beam;
import configparser
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import argparse
import logging
import re
import os
from datetime import datetime, timedelta,date
import time
from string import lower
from string import upper
from apache_beam import pvalue
from google.cloud import bigquery
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
datasetname='boat'
projectid='insights-sandbox-153010'

def wait_for_job(job):
        credentials = GoogleCredentials.get_application_default()
        bq = discovery.build('bigquery', 'v2', credentials=credentials) 
        logging.info('Waiting for job to finish...')
        request = bq.jobs().get(
            projectId=job['jobReference']['projectId'],
            jobId=job['jobReference']['jobId'])
        result = request.execute()
        while result['status']['state'] != 'DONE':
            result = request.execute()        
        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                logging.exception('Error occurred while loading data in '+outputTableName+" : \n Reason"+result['status']['errorResult']['reason']+"\n message"+result['status']['errorResult']['message']+"\nQuery : "+result['configuration']['query']['query'])
                raise RuntimeError(result['status']['errorResult'])
            print("job completed successfully")
            
def copy_data_to_partitioned(table_name,target_table, partitionId,load_date):
    """Copies a table.

    If no project is specified, then the currently active project is used.
    """
    if True:
        target_table_partition=target_table+"$"+partitionId
        logging.info("Partition "+target_table_partition+" is being loaded..")
        copy_Query='select * from `'+datasetname+"."+table_name+"` where date_id = date('"+load_date+"')"
        print(copy_Query)
        bigqclient = bigquery.Client(project=projectid)
        tdatasetname = bigqclient.dataset(datasetname)
        table = tdatasetname.table(target_table)
        if(table.exists()) :
            tablefields=[]
            table.reload()
            tableschema = table.schema
            for fields in tableschema:
                    tablefields.append(fields.name)
            query_data = {
                    'configuration': {
                        'query': {
                            'query': copy_Query,
                            'destinationTable': {
                                'projectId': projectid,
                                'datasetId': datasetname,
                                'tableId': target_table_partition                
                            },
                            'createDisposition': 'CREATE_IF_NEEDED',
                            'writeDisposition': 'WRITE_TRUNCATE',
                            'allowLargeResults': True,
                            "useLegacySql" : False
                        },
                        "tableDefinitions": {
                              "schema": tablefields
                        }
                        
                    }
                }
            credentials = GoogleCredentials.get_application_default()
            bq = discovery.build('bigquery', 'v2', credentials=credentials)
            
            job=bq.jobs().insert(projectId=projectid,body=query_data).execute()
             
            logging.info('Waiting for job to finish...')
             
            request = bq.jobs().get(
                    projectId=job['jobReference']['projectId'],
                    jobId=job['jobReference']['jobId'])
            result = request.execute()
            while result['status']['state'] != 'DONE':
                result = request.execute()
            if result['status']['state'] == 'DONE':
                if 'errorResult' in result['status']:
                    logging.exception('Error occurred while loading data in '+target_table+" : \n Reason"+result['status']['errorResult']['reason']+"\n message"+result['status']['errorResult']['message']+"\nQuery : "+result['configuration']['query']['query'])
                    raise RuntimeError(result['status']['errorResult'])   
                logging.info("Partition "+target_table_partition+" Truncated")
        else:
            logging.info("Table "+target_table+" not exists")   
    print(('Table {} copied to {}.'.format(table_name, target_table+partitionId)))
    
table_name='stg1_evs_an_spend2_load_history'  
target_table='stg1_evs_an_spend2_load_history'
 
for partitiondate in ['2017-03-22']  :
    partitionId=datetime.date(datetime.strptime(partitiondate,"%Y-%m-%d")).strftime("%Y%m%d")
    print(partitionId)  
    copy_data_to_partitioned(table_name,target_table, partitionId, partitiondate)
#
#download_info
    