'''
Created on March 29, 2018

@author: Rajnikant Rakesh
This module returns python List of table fields for a table in BigQuery.
It does not read from file it reads from actual table definition from BigQuery 

'''

import argparse
import configparser
import argparse
import logging
import re
import os
from datetime import datetime, timedelta,date
import time
from google.cloud import bigquery
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']

def load_csv_to_bigquery_using_bqsdk(projectid,
                datasetname,
                csv_file_path,
                outputTableName,
                fieldDelimiter,
                skipLeadingRows,
                writeDeposition):
        """This function will read data from csv and load in BigQuery destination table using BigQuery sdk insert job."""
        logging.info('Loading table '+outputTableName)
        print('Loading table {}'.format(outputTableName))
        try:
            #credentials = GoogleCredentials.get_application_default()
            bigqclient = bigquery.Client(project=projectid)#, credentials=credentials)
            tdatasetname = bigqclient.dataset(datasetname)
            table_ref = tdatasetname.table(outputTableName)
            #table.reload()
            table = bigqclient.get_table(table_ref)
            tableschema = table.schema
            outputTableSchema = []
            for fields in tableschema:
                outputTableSchema.append(fields.name)
        except:
            logging.exception('Target table is not created ('+datasetname+"."+outputTableName+"). Please create it first.")
            raise RuntimeError('Target table is not created ('+datasetname+"."+outputTableName+"). Please create it first.")
        try:
            query_data = {
                'configuration':{  
                            'load':{  
                                'ignoreUnknownValues':False,
                                'skipLeadingRows':skipLeadingRows,
                                'sourceFormat':'CSV',
                                'destinationTable':{  
                                    'projectId':projectid,
                                    'datasetId':datasetname,
                                    'tableId':outputTableName
                                    },
                                'maxBadRecords':1,
                                'allowJaggedRows':False,
                                'writeDisposition':'WRITE_TRUNCATE',
                                'sourceUris':[csv_file_path],
                                'fieldDelimiter':fieldDelimiter,
                                #'allowQuotedNewlines':singlequoted,
                                'schema':outputTableSchema#,
                                #'encoding':encoding
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
                    for error in result['status']['errors']:
                        logging.exception('reason :'+error['reason']+',message :'+error['message'])
                    raise RuntimeError(result['status']['errorResult'])
                logging.info("Data loading completed successfully [Source:"+csv_file_path+", Destination:"+outputTableName+"]")
                print("Data loaded successfully [Source:{}, Destination:{}]".format(csv_file_path, outputTableName))
        except RuntimeError:
            raise

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)