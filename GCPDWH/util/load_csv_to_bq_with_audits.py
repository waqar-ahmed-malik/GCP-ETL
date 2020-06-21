'''
Created on March 28, 2018
@author: Ramneek Kaur
This module reads data from Oracle and Write into Bigquery Table
The data is staged as csv in GCS Buckets
Version 1.1
@author: Atul Guleria
Added methods to download and updload csv file in GCS. Made script to handle extra columns to be added.
'''

import datetime
import os
import argparse
import csv
from google.cloud import bigquery
import logging
import sys
import apache_beam as beam
from pandas.io.tests.parser import header
from pandas.io.sas.sas_constants import index
from pandas.indexes.base import Index
from google.cloud import storage as gstorage
from google.cloud.storage import blob as Blob
from googleapiclient import discovery
import pandas
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.cloud import storage
import csv
import time
from string import lower

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
        print('here')
        try:
            #credentials = GoogleCredentials.get_application_default()
            bigqclient = bigquery.Client(project=projectid)#, credentials=credentials)
            tdatasetname = bigqclient.dataset(datasetname)
            table_ref = tdatasetname.table(outputTableName)
            #table.reload()
            print('here')
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
                                'maxBadRecords':10,
                                'allowJaggedRows':False,
                                'writeDisposition':writeDeposition,
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

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name)))
    
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name)))  

def add_columns(file_name, schemafile):
    reader = pandas.read_csv(schemafile, delimiter=',')
    for column in reader:
        df = pandas.read_csv(file_name, header= None )
        df[column] = column 
          
        df.to_csv(os.path.abspath("GCPDWH/"+product_config['productname']+'/data/'+filename),columns=None, header=False, index=False, index_label=None ) 
        
def add_audit_columns(file_name):
        df = pandas.read_csv(file_name, header= None )
#         df[job_id] = jobId 
        ts = time.gmtime()
        df['create_dt'] = time.strftime("%Y-%m-%d %H:%M:%S", ts) 
        df['create_by'] = lower("Load-" + product_config['datasetname'].replace("_", "") + "-" + targettable.replace("_", "") )
          
        df.to_csv(os.path.abspath("GCPDWH/"+product_config['productname']+'/data/'+filename),columns=None, header=False, index=False, index_label=None )         

def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
    """Copies a blob from one bucket to another with a new name."""
    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)

    print(('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
        source_blob.name, source_bucket.name, new_blob.name,
        destination_bucket.name)))
    
def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    print(blob)
    blob.delete()

    print(('Blob {} deleted.'.format(blob_name)))    
    
def run(): 
    try:
        exec(compile(open(os.path.abspath("GCPDWH/util/readtableschema.py")).read(), os.path.abspath("GCPDWH/util/readtableschema.py"), 'exec'), globals())
        readtableschema (env_config['projectid'],product_config['datasetname'] ,targettable)
        gs_source_csv_filepath="gs://"+product_config['bucket']+filename
        print("Generated CSV File => data/{}.csv".format(filename))
        print(gs_source_csv_filepath)
#         print os.path.abspath("GCPDWH/"+product_config['productname']+'/data/'+filename)
                              
        if addauditcols == 1:
         download_blob(product_config['bucket'], 
                      'data/'+filename, 
                      os.path.abspath("GCPDWH/"+product_config['productname']+'/data/'+filename))
         if addcols == 1:
          add_columns(filenamerepo,schemafile)   
         
         add_audit_columns(filenamerepo)
               
         upload_blob(product_config['bucket'], 
                      os.path.abspath("GCPDWH/"+product_config['productname']+'/data/'+filename), 
                      'data/'+filename)
        projectname=env_config['projectid'] 
        exec(compile(open(os.path.abspath("GCPDWH/util/loadcsvtobigqueryusingbqsdk.py")).read(), os.path.abspath("GCPDWH/util/loadcsvtobigqueryusingbqsdk.py"), 'exec'), globals())      
        

        load_csv_to_bigquery_using_bqsdk(projectname,
                product_config['datasetname'],
                gs_source_csv_filepath,
                targettable,
                separatorchar,
                skipleadingrows,
                writedeposition)
#          
#         copy_blob(product_config['bucket'], 
#                   'data/'+filename, 
#                   product_config['bucket'], 
#                   'archive/'+filename)
#         
#         delete_blob(product_config['bucket'], 
#                     'data/'+filename)
        
        print("\nrefresh ended for product: " + product_config['datasetname'])
    except:
        logging.exception('Unable to Write data into BigQuery Table ')
        raise
   
    
def main(args_config,args_productconfig,args_env,args_targettable,args_writeDeposition,args_filename,args_separator,args_skipleadingrows,args_addcols,args_addauditcols,args_schemafile):
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(os.path.abspath("GCPDWH/util/readconfig.py")).read(), os.path.abspath("GCPDWH/util/readconfig.py"), 'exec'), globals())    
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global targettable
    targettable = args_targettable
    global writedeposition
    writedeposition = args_writeDeposition
    global filename
    filename = args_filename
    global separatorchar
    separatorchar = args_separator
    global skipleadingrows
    skipleadingrows = args_skipleadingrows
    global addcols
    addcols = args_addcols
    if args_addcols == 1:
     global schemafile
     schemafile = os.path.abspath("GCPDWH/"+product_config['productname']+'/schema/'+args_schemafile)
     global filenamerepo
     filenamerepo = os.path.abspath("GCPDWH/"+product_config['productname']+'/data/'+filename)
    global addauditcols
    addauditcols = args_addauditcols 
    if args_writeDeposition=='WRITE_TRUNCATE':
        writedeposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    else:
        writedeposition=beam.io.BigQueryDisposition.WRITE_APPEND
        print(writedeposition)
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
        '--writeDeposition',
        required=False,
        default='WRITE_APPEND',
        help= ('WRITE_APPEND by Default, WRITE_TRUNCATE to truncate')
      )
    parser.add_argument(
        '--filename',
        required=True,
        help= ('filename from which data would be loaded')
        )
    parser.add_argument(
        '--separator',
        required=True,
        help= ('Separator character used in source file (gcs file)')
        )   
    parser.add_argument(
        '--skipleadingrows',
        required=True,
        help= ('number to rows to be skipped')
        )     
    parser.add_argument(
        '--addcols',
        required=True,
        help= ('whether to add extra columns or not')
        )     
    parser.add_argument(
        '--addauditcols',
        required=True,
        help= ('whether to add audit columns or not')
        )                 
    parser.add_argument(
        '--schemafile',
        required=False,
        help= ('schemafile from which additional columns would be loaded')
        )
    args = parser.parse_args()   
    
    
    main(args.config,
         args.productconfig,
         args.env,
         args.targettable,
         args.writeDeposition,
         args.filename,
         args.separator,
         args.skipleadingrows,
         args.addcols,
         args.addauditcols,
         args.schemafile
         )
