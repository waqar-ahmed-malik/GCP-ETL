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

def truncate_partition(projectid,datasetname,target_table,partitionId):    
    """Truncate bigquery table partition"""
    target_table_partition=target_table+"$"+partitionId
    logging.info("Partition "+target_table_partition+" is being truncated..")
    empty_Query='select * from '+datasetname+"."+target_table+' where 1=2'
    bigqclient = bigquery.Client(project=projectid)
    tdatasetname = bigqclient.dataset(datasetname)
    table = tdatasetname.table(target_table)
    if(table.exists()) :
        tablefields=[]
        table.reload()
        tableschema = table.schema
        for fields in tableschema:
                tablefields.append(fields.name)
        bigquery.Client(project=projectid)
        query_data = {
                'configuration': {
                    'query': {
                        'query': empty_Query,
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
                logging.exception('Error occurred while loading data in '+outputTableName+" : \n Reason"+result['status']['errorResult']['reason']+"\n message"+result['status']['errorResult']['message']+"\nQuery : "+result['configuration']['query']['query'])
                raise RuntimeError(result['status']['errorResult'])   
            logging.info("Partition "+target_table_partition+" Truncated")
    else:
        logging.info("Table "+target_table+" not exists")   
    return list("1")

truncate_partition('insights-sandbox-153010','boat','stg1_evs_an_spend2_load_history','20170406')

