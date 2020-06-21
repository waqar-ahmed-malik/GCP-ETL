'''
Created on June 27, 2018
@author: Rajnikant Rakesh
This module reads data from Oracle and Write into Bigquery Table
It uses pandas dataframe to read and write data to BigQuery.
'''

 
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

class setenv(beam.DoFn): 
      def process(self,context):
          os.system('mkdir /tmp/oracle && /usr/bin/toolbox')
          os.system('gsutil -m cp -r '+product_config['stagingbucket']+'/instantclient_18_3/ /tmp/oracle/ && apt-get install libaio1 ' )
          
          #os.system('gsutil cp '+product_config['stagingbucket']+'/ojdbc7.jar /tmp/')
          logging.info('Instant Cleint copied to Instance..')
          
          #os.system('gsutil -m cp -r '+product_config['stagingbucket']+'/jvm/jdk1.8.0_181/ /usr/lib/')
          #logging.info('Java Libraries copied to Instance..')
          os.environ["LD_LIBRARY_PATH"]='/tmp/oracle/instantclient_18_3:$LD_LIBRARY_PATH'
          os.environ["PATH"]='$PATH:/tmp/oracle/instantclient_18_3'
          logging.info('Enviornment Variable set.')
          return list("1")
      
def readfileasstring(sqlfile):   
    """
    Read any text file and return text as a string. 
    This function is used to read .sql and .schema files
    """ 
    with open (sqlfile, "r") as file:
         sqltext=file.read().strip("\n").strip("\r")
    return sqltext

class readfromoracle(beam.DoFn): 
      def process(self,context,sql):
          
          print(sql)
          logging.info('Query is %s' , sql)
          database_user=env_config[connectionprefix+'_database_user']
          database_password=env_config[connectionprefix+'_database_password']
          database_host=env_config[connectionprefix+'_database_host']
    
          cnx = cx_Oracle.connect(database_user+"/"+database_password+"@"+database_host)
          cnx.cursor()
          logging.info('Reading Sql Query from the file...')
          query = sql.replace('v_incr_date',incrementaldate).replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
          logging.info('Query is %s',query)
          logging.info('Query submitted to Oracle Database')
          pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None )
#          print queryresult.dtypes
          for chunk in pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=100000):
            chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='append')
    
          logging.info("Load completed...")
          return list("1")
    #,if_exists='append'

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
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])
        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())
        #connectdb= pcoll | 'Connecting Database' >> beam.Create(['C:\cloud_sdk\GCPDWH\cts\sql\insert_work_cts_complaint_dim.sql'])
#        readsql = (connectdb | 'Reading Sql' >> beam.Map(readfileasstring))
        (dummy_env | 'Processing' >>  beam.ParDo(readfromoracle(),sqlstring[0]))
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
    global sqlstring
    jobname="load-"+product_config['productname']+"-"+"0713"
    logging.info('Job Name is %s',jobname)
    sqlstring= readfileasstring('C:\cloud_sdk\GCPDWH\cts\sql\insert_work_cts_complaint_dim.sql').split(';')
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