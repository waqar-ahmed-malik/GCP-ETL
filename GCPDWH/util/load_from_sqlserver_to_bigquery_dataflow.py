'''
Created on June 27, 2018
@author: Rajnikant Rakesh
This module reads data from Oracle and Write into Bigquery Table
It uses pandas dataframe to read and write data to BigQuery.
'''

 
import jaydebeapi
import apache_beam as beam
import datetime
import cx_Oracle
import os
import argparse
from google.cloud import bigquery
import logging
import sys
from string import lower
from google.cloud import storage as gstorage
import pandas as  pd
from oauth2client.client import GoogleCredentials
import pytz

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

print(utilpath)

def readfileasstring(sqlfile):   
    """
    Read any text file and return text as a string. 
    This function is used to read .sql and .schema files
    """ 
    with open (sqlfile, "r") as file:
         sqltext=file.read().strip("\n").strip("\r")
    return sqltext

class readfromoracle(beam.DoFn): 
      def process(self,element):
          print(element)
          database_user=env_config[connectionprefix+'_database_user']
          database_password=env_config[connectionprefix+'_database_password']
          database_host=env_config[connectionprefix+'_database_host']
          os.system('gsutil cp gs://dw-prod-gcpprocess/sqljdbc41.jar /tmp/')
          jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
          url = ("jdbc:sqlserver://" + database_host + ';database=' + database_database + ';user=' + database_user + ';password=' + database_password)
          jars = ["/tmp/sqljdbc41.jar"]
          libs = None
          cnx = jaydebeapi.connect(jclassname, url, jars=jars,
                            libs=libs)   
          
          cnx.cursor()
          logging.info('Reading Sql Query from the file...')
          query = readfileasstring(sqlfile).replace('v_incr_date',incrementaldate)
          logging.info('Query is %s',query)
          logging.info('Query submitted to SqlServer Database')
          logging.info("Started loading Table")
          for chunk in pandas.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=100000):
              chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='replace')
   
          logging.info("Load completed...")

def run():
    """
    1. Set Dataflow PipeLine configurations.
    2. Create PCollection element for each line read from the delimited file.
    3. Tag values by calling RowValidator method i.e. clean records or broken records.
    4. Call rowextractor method for cleaned records.
    5. Write valid/clean records to BigQuery table mentioned in the parameter.
    6. Sink the error records to error handler.  
    """
    
    pipeline_args = ['--project', env_config['projectid'],
                     '--job_name', jobname,
                     '--runner', env_config['runner'],
                     '--staging_location', product_config['stagingbucket'],
                     '--temp_location', product_config['tempbucket'],
                     '--requirements_file', env_config['requirements_file'],
                     '--region', env_config['region'],
                     '--zone',env_config['zone'],
                     '--network',env_config['network'],
                     '--subnetwork',env_config['subnetwork'],
                     '--save_main_session', 'True',
                     '--num_workers', env_config['num_workers'],
                     '--max_num_workers', env_config['max_num_workers'],
                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],
                     '--service_account_name', env_config['service_account_name'],
                     '--service_account_key_file', env_config['service_account_key_file']
                     ]
    
    try:

        pcoll = beam.Pipeline(argv=pipeline_args)
        connectdb= pcoll | 'Connecting Database' >> beam.Create(['C:\cloud_sdk\GCPDWH\ers\sql\sc_status.sql'])
        readsql = (connectdb | 'Reading Sql' >> beam.Map(readfileasstring))
        (readsql | 'Processing' >>  beam.ParDo(readfromoracle()))
        pcoll.run()
        #pcoll.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise    

def main(args_config,args_productconfig,args_env,args_targettable,args_sqlquery,args_connectionprefix,args_incrementaldate):
    logging.getLogger().setLevel(logging.INFO)
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(utilpath+'readconfig.py').read(), utilpath+'readconfig.py', 'exec'), globals())
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global targettable
    targettable = args_targettable
    global sqlfile
    sqlfile = args_sqlquery
    global connectionprefix
    connectionprefix = args_connectionprefix
    global incrementaldate
    incrementaldate=args_incrementaldate
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    jobname="load-"+product_config['productname']+"-"+"0713"
    logging.info('Job Name is %s',jobname)
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
        '--sqlquery',
        required=True,
        help= ('sqlquery from which data needs to be extracted')
        )
         
    parser.add_argument(
        '--connectionprefix',
        required=True,
        help= ('connectionprefix either Schema')
        )
    
    parser.add_argument(
        '--incrementaldate',
        required=True,
        help= ('Incremental Data Pull Filter date')
        )

    args = parser.parse_args()   
        
    main(args.config,
         args.productconfig,
         args.env,
         args.targettable,
         args.sqlquery,
         args.connectionprefix,
         args.incrementaldate)    