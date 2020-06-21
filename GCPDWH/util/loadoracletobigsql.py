
'''
Created on March 16, 2018
@author: Rajnikant Rakesh
This module reads data from Oracle and Write into Bigquery Table
The data is staged as csv in GCS Buckets

'''
import datetime
#import mysql.connector
import cx_Oracle
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

util_path = os.path.dirname(os.path.abspath(__file__))

def load_csv_to_bigquery_using_bqsdk(projectid,
                datasetname,
                csv_file_path,
                outputTableName,
                #outputTableSchema,
                fieldDelimiter=",",
                skipLeadingRows=0,
                writeDeposition='WRITE_TRUNCATE'):
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
                                'maxBadRecords':0,
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
    "run query, generate csv and loads in targettable"
    try:
#         database_user=env_config[connectionprefix+'_database_user']
#         database_password=env_config[connectionprefix+'_database_password']
#         database_host=env_config[connectionprefix+'_database_host']
        
        cnx = cx_Oracle.connect('A603435/qant552@ETLQ')
        cnx.cursor()
        #query = loaddaily.readfileasstring(mysqlquery)
        query="select pay_id,mbr_id,pay_date,pay_type from usage.payment where rownum<100"
        print("Using following SQL to get results")
        print(query)
        print("\n refresh started for product: " + product_config['datasetname'])
        if not os.path.exists('data'):
            os.makedirs('data')
        source_csv_filename=product_config['datasetname']+targettable+'.csv'
        source_csv_filepath='data/'+source_csv_filename
        queryresult=pandas.read_sql(query, cnx, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
        try:
            os.remove(source_csv_filepath)
        except OSError:
            pass
        
        f=open(source_csv_filepath, 'wb')
        queryresult.to_csv(f, encoding='utf-8',float_format='%.10g', header=True, quoting=1,index=False)    
        f.close()
        cnx.close()
        exec(compile(open(util_path+'//readtableschema.py').read(), util_path+'//readtableschema.py', 'exec'), globals())
        tablefieldcolumns=readtableschema (env_config['projectid'],product_config['datasetname'] ,targettable)
        
        for field in tablefieldcolumns :
            if not field in queryresult.columns:
                queryresult[field]=""
                
        #Reorder columns in order of tablefields        
        queryresult = queryresult[tablefieldcolumns]
                
        try:
            os.remove(source_csv_filepath)
        except OSError:
            pass        
        f=open(source_csv_filepath, 'wb')
        queryresult.to_csv(f, encoding='utf-8',float_format='%.10g', header=True, quoting=1,index=False)    
        f.close()        
                
        
        print("Generated CSV File => data/{}.csv".format(source_csv_filename))
    
        exec(compile(open(util_path+'//loadcsvtobigqueryusingbqsdk.py').read(), util_path+'//loadcsvtobigqueryusingbqsdk.py', 'exec'), globals())
        
        projectname=env_config['projectid']
        storageclient = gstorage.Client(project=projectname)
        bucket = storageclient.get_bucket(product_config['bucket'])
        blob = bucket.blob(source_csv_filename)
        blob.upload_from_filename("C://GCP//GCPDWH//data//"+source_csv_filename)
        
        gs_source_csv_filepath="gs://"+product_config['bucket']+"/"+source_csv_filename
     
        load_csv_to_bigquery_using_bqsdk(projectname,
                product_config['datasetname'],
                gs_source_csv_filepath,
                targettable,
                #outputTableSchema,
                fieldDelimiter=",",
                skipLeadingRows=1,
                writeDeposition='WRITE_TRUNCATE')
        
        print("\nrefresh ended for product: " + product_config['datasetname'])
    except:
        logging.exception('Unable to fetch data from MySQL and create csv file and unable to load data into table ')
        raise
    
def main(args_config,args_productconfig,args_env,args_targettable,args_connectionprefix):
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
    #global mysqlquery
   # mysqlquery = args_mysqlquery
    global connectionprefix
    connectionprefix = args_connectionprefix
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
    """
    parser.add_argument(
        '--mysqlquery',
        required=True,
        help= ('mysqlquery from which data needs to be extracted')
        )
    """     
    parser.add_argument(
        '--connectionprefix',
        required=True,
        help= ('connectionprefix either baas or caas')
        )

    args = parser.parse_args()   
    
    
    main(args.config,
         args.productconfig,
         args.env,
         args.targettable,
     #    args.mysqlquery,
         args.connectionprefix)

