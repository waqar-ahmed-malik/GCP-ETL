'''
Created on June 6, 2018
@author: Prerna Anand
This module reads data from GCS and Write into Bigquery Table
The data is staged as csv in GCS Buckets
'''

import datetime
import os
import argparse
import csv
from google.cloud import bigquery
import logging
import sys
sys.path.append(os.path.abspath('..\deployment'))
from google.cloud import storage as gstorage
from google.cloud.storage import blob as Blob
from googleapiclient import discovery
import pandas
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def load_csv_to_bigquery_using_bqsdk(projectid,
                datasetname,
                csv_file_path,
                outputTableName,
                #outputTableSchema,
                fieldDelimiter="|",
                skipLeadingRows=1,
                writeDeposition='WRITE_TRUNCATE'):
        """This function will read data from csv and load in BigQuery destination table using BigQuery sdk insert job."""
        logging.info('Loading table '+outputTableName)
        print('Loading table {}'.format(outputTableName))
        try:
            
            bigqclient = bigquery.Client(project=projectid)
            tdatasetname = bigqclient.dataset(datasetname)
            table_ref = tdatasetname.table(outputTableName)
            
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
                                'allowQuotedNewlines':False,
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
        exec(compile(open('C://Projects//AAA//cloud_sdk//GCPDWH//util//readtableschema.py').read(), 'C://Projects//AAA//cloud_sdk//GCPDWH//util//readtableschema.py', 'exec'), globals())
        readtableschema (env_config['projectid'],product_config['datasetname'] ,targettable)
        
      
        source_csv_filename='DNC_Combined_All_Files_012618.CSV'
        gs_source_csv_filepath="gs://"+product_config['bucket']+"/"+source_csv_filename
        
        print("Generated CSV File => data/{}.csv".format(source_csv_filename))
    
        exec(compile(open('C://Projects//AAA//cloud_sdk//GCPDWH/util//loadcsvtobigqueryusingbqsdk.py').read(), 'C://Projects//AAA//cloud_sdk//GCPDWH/util//loadcsvtobigqueryusingbqsdk.py', 'exec'), globals())
        
        
        projectname=env_config['projectid']
        
        load_csv_to_bigquery_using_bqsdk(projectname,
                product_config['datasetname'],
                gs_source_csv_filepath,
                targettable,
                #outputTableSchema,
                fieldDelimiter="|",
                skipLeadingRows=1,
                writeDeposition='WRITE_TRUNCATE')
        
        print("\nrefresh ended for product: " + product_config['datasetname'])
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
    exec(compile(open('C://Projects//AAA//cloud_sdk//GCPDWH/util//readconfig.py').read(), 'C://Projects//AAA//cloud_sdk//GCPDWH/util//readconfig.py', 'exec'), globals())
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global targettable
    targettable = args_targettable
    
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
