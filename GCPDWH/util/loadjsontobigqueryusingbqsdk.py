'''
Created on March 20, 2018

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

def loadjsontobigqueryusingbqsdk(projectid,
                datasetname,
                json_file_path,
                outputTableName,
                outputtablepartitionid,
                writeDeposition='WRITE_TRUNCATE'):
        """This function will read data from csv and load in BigQuery destination table using BigQuery sdk insert job."""
        logging.info('Loading table '+outputTableName)  
        bigqclient = bigquery.Client(project=projectid)
        tdatasetname = bigqclient.dataset(datasetname)
        table = tdatasetname.table(outputTableName)
        if(table.exists()) :
            tablefields=[]
            table.reload()
            tablefields = []
            for fields in table.schema:
                tablefields.append(fields.name)   
            outputTableSchema = tablefields    
        else:
            logging.exception('Target table is not created ('+datasetname+"."+outputTableName+"). Please create it first.")
            raise RuntimeError('Target table is not created ('+datasetname+"."+outputTableName+"). Please create it first.")
        if outputtablepartitionid !=None :
            outputTableName=outputTableName+"$"+outputtablepartitionid
        logging.info('Loading table '+outputTableName)  
        query_data = {
            'configuration':{  
                        'load':{  
                                'ignoreUnknownValues':False,
                                'sourceFormat':'NEWLINE_DELIMITED_JSON',
                                'destinationTable':{  
                                    'projectId':projectid,
                                    'datasetId':datasetname,
                                    'tableId':outputTableName
                                    },
                                'maxBadRecords':0,
                                'allowJaggedRows':False,
                                'writeDisposition':writeDeposition,
                                'sourceUris':[json_file_path],
                                'schema':outputTableSchema
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
        
        inputFiles='NA'
        recordsWritten='NA'
        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                
                logging.exception('Error occurred while loading data in '+outputTableName+" : \n Reason"+result['status']['errorResult']['reason']+"\n message"+result['status']['errorResult']['message'])
                for error in result['status']['errors']:
                     logging.exception('reason :'+error['reason']+',message :'+error['message'])
                raise RuntimeError(result['status']['errorResult'])
            try:
                inputFiles=result['statistics']['load']['inputFiles']
                recordsWritten=str(result['statistics']['load']['outputRows'])
                startTime=datetime.fromtimestamp(float(result['statistics']['startTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')
                endTime=datetime.fromtimestamp(float(result['statistics']['endTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')      
                logging.info('Job completed successfully. Table '+outputTableName+" [Write : "+recordsWritten+"]")  
                logging.info("BiqQuery Statistics : \nInputFiles : "+inputFiles+" \n recordsWritten : "+recordsWritten+"\n startTime : "+startTime+"\n endTime : "+endTime)               
            except KeyError:
                print(result)
                logging.info('Job execution completed. Key Error occurred while accessing statistics. '+outputTableName+" [Read : NA, Write : NA]")  
                logging.info("BiqQuery Statistics Not Available")
        
        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            logging.info("Data loading completed successfully [Source:"+json_file_path+", Destination:"+outputTableName+"]")
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    
    