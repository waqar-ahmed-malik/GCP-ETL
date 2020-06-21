'''
Created on March 28, 2018
@author: Ramneek Kaur
This module reads data from Oracle and Write into Bigquery Table
The data is staged as csv in GCS Buckets

Revision 
July 7,2018
 6/13
@author: Rajnikant Rakesh 
1. Added util_path for relative path setting.
2. Added Explicit authentication against GCP project.
3. Delimiter and 

'''

import datetime
import os
import argparse
import csv
from google.cloud import bigquery
import logging
import sys
from google.cloud import storage as gstorage
from google.cloud.storage import blob as Blob
from googleapiclient import discovery
import pandas
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

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
                                #'allowQuotedNewLines':True,
                                'encoding':'ISO-8859-1',
                                'maxBadRecords':100,
                                'allowJaggedRows':False,
                                'writeDisposition':writeDeposition,
                                'sourceUris':[csv_file_path],
                                'fieldDelimiter':fieldDelimiter
#                                 'schema':outputTableSchema
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

def run(): 
    try:
        exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals()) 
        logging.info('Reading Table schema for %s',targettable)
        readtableschema (env_config['projectid'],product_config['datasetname'] ,targettable)
        gs_source_csv_filepath="gs://"+product_config['bucket']+"/"+filename
        projectname=env_config['projectid']
        
        load_csv_to_bigquery_using_bqsdk(projectname,
                product_config['datasetname'],
                gs_source_csv_filepath,
                targettable,
                fieldDelimiter,
                skipLeadingRows,
                writeDeposition)
        
        print("\nrefresh ended for product: " + product_config['datasetname'])
    except:
        logging.exception('Unable to Write data into BigQuery Table ')
        raise
   
    
def main(args_config,args_productconfig,args_env,args_targettable,args_filename,args_delimiter,args_deposition,args_skiprows):
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals()) 
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global targettable
    targettable = args_targettable
    global filename
    filename = args_filename
    global fieldDelimiter
    fieldDelimiter=args_delimiter
    global writeDeposition
    writeDeposition=args_deposition
    global skipLeadingRows
    skipLeadingRows=args_skiprows
    
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
    parser.add_argument(
        '--filename',
        required=True,
        help= ('filename from which data would be loaded')
        )
    parser.add_argument(
        '--delimiter',
        required=True,
        help= ('Delimiter in the file ,| etc.')
        )
    parser.add_argument(
        '--deposition',
        required=True,
        help= ('WRITE_APPEND, WRITE_TRUNCATE etc')
        )
    parser.add_argument(
        '--skiprows',
        required=True,
        help= ('No. of leading rows to skip')
        )
    args = parser.parse_args()   
       
    main(args.config,
         args.productconfig,
         args.env,
         args.targettable,
         args.filename,
         args.delimiter,
         args.deposition,
         args.skiprows
         )
