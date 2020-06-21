'''
Created on April 09, 2018

@author: Rajnikant Rakesh
This module reads data from bigquery tables/Queries and loads into Bigquery tables.

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

util_path = os.path.dirname(os.path.abspath(__file__))

def readfileasstring(sqlfile):   
    """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 
    with open (sqlfile, "r") as file:
        sqltext=file.read().strip("\n").strip("\r")
    return sqltext

def loadbigquerytobigqueryusingbqsdk(projectid,
            datasetname,
            inputQuery,
            outputTableName,
            #outputtablepartitionid,
            createDisposition='CREATE_IF_NEEDED',
            writeDeposition='WRITE_APPEND'):
    """This function will read data using query and load in BigQuery destination table using BigQuery sdk insert job."""
    bigqclient = bigquery.Client(project=projectid)#, credentials=credentials)
    tdatasetname = bigqclient.dataset(datasetname)
    table_ref = tdatasetname.table(outputTableName)
            #table.reload()
    table = bigqclient.get_table(table_ref)
    tableschema = table.schema
    outputTableSchema = []
    for fields in tableschema:
            outputTableSchema.append(fields.name) 
        
    logging.info('Loading table '+outputTableName) 
    query_data = {
        'configuration': {
            'query': {
                'query': inputQuery,
                'destinationTable': {
                    'projectId': projectid,
                    'datasetId': datasetname,
                    'tableId': outputTableName                
                },
                'createDisposition': createDisposition,
                'writeDisposition': writeDeposition,
                'allowLargeResults': True,
                "useLegacySql" : False
            },
            "tableDefinitions": {
                  "schema": outputTableSchema
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
    
    recordsRead='NA'
    recordsWritten='NA'
    status='NA'
    query='NA'
    if result['status']['state'] == 'DONE':
        if 'errorResult' in result['status']:
            logging.exception('Error occurred while loading data in '+outputTableName+" : \n Reason"+result['status']['errorResult']['reason']+"\n message"+result['status']['errorResult']['message'])
            for error in result['status']['errors']:
                logging.exception('reason :'+error['reason']+',message :'+error['message'])
            raise RuntimeError(result['status']['errorResult'])
        try:
            recordsRead=str(result['statistics']['query']['queryPlan'][-1]['recordsRead'])
            recordsWritten=str(result['statistics']['query']['queryPlan'][-1]['recordsWritten'])
            status=result['statistics']['query']['queryPlan'][-1]['status']  
            query=result['configuration']['query']['query']
            startTime=datetime.fromtimestamp(float(result['statistics']['startTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')
            endTime=datetime.fromtimestamp(float(result['statistics']['endTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')      
            totalBytesProcessed=str(int(result['statistics']['totalBytesProcessed'])/1048576)+" MB"           
            totalBytesBilled=str(int(result['statistics']['query']['totalBytesBilled'])/1048576)+" MB"
            cacheHit=str(result['statistics']['query']['cacheHit'])
            billingTier=str(result['statistics']['query']['billingTier'])
            logging.info('Job completed successfully. Table '+outputTableName+" [Read : "+recordsRead+", Write : "+recordsWritten+"]")  
            logging.info("BiqQuery Statistics : \n recordsRead : "+recordsRead+"\n recordsWritten : "+recordsWritten+"\n status : "+status+"\n startTime : "+startTime+"\n endTime : "+endTime+"\n totalBytesProcessed : "+totalBytesProcessed+"\n totalBytesBilled : "+totalBytesBilled+"\n cacheHit : "+cacheHit+"\n billingTier : "+billingTier+"\n query : "+query)                
        except KeyError:
            logging.info('Job execution completed. Key Error occurred while accessing statistics. '+outputTableName+" [Read : NA, Write : NA]")  
            logging.info("BiqQuery Statistics Not Available")
            
def run(): 
    try:
        exec(compile(open(util_path+'//readtableschema.py').read(), util_path+'//readtableschema.py', 'exec'), globals())
        tablefieldcolumns=readtableschema (env_config['projectid'],product_config['datasetname'] ,targettable)
        source_csv_filename='AAANCNU_Data_20180324.psv'
        projectname=env_config['projectid']
        inpQuery=readfileasstring('C://GCP//GCPDWH//util//sql//transactionfacttst.sql')
        
        loadbigquerytobigqueryusingbqsdk(projectname,
            product_config['datasetname'],
            inpQuery,
            targettable,
            createDisposition='CREATE_IF_NEEDED',
            writeDeposition='WRITE_APPEND')
              
        print("\nRefresh ended for: " + product_config['datasetname'] +"."+ targettable)
    except:
        logging.exception('Unable to Write data into BigQuery Table ')
        raise
   
    
def main(args_config,args_productconfig,args_env,args_targettable):
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(util_path+'//readconfig.py').read(), util_path+'//readconfig.py', 'exec'), globals())
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global targettable
    targettable = args_targettable
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file'] 
    run()  
           
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--config',
        required=True,
        help= ('Config file name')
        )
    parser.add_argument(
        '--productconfig',
        required=True,
        help= ('product Config file name')
        )
    parser.add_argument(
        '--env',
        required=True,
        help= ('Enviornment to be run dev/test/prod')
        )
    parser.add_argument(
        '--targettable',
        required=True,
        help= ('targettable to which data would be loaded')
        )
  
    args = parser.parse_args()   
    
    
    main(args.config,
         args.productconfig,
         args.env,
         args.targettable
         )
