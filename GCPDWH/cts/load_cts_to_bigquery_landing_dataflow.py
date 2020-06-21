'''
Created on Sep 27, 2018
@author: Rajnikant Rakesh
This module reads data from Oracle and Write into Bigquery Table
It uses pandas dataframe to read and write data to BigQuery.
'''

 
import apache_beam as beam
import time
import jaydebeapi 
import os
import argparse
from google.cloud import bigquery
import logging
import sys
import base64
from google.cloud import storage as gstorage
import pandas as  pd
from oauth2client.client import GoogleCredentials


cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
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

class setenv(beam.DoFn): 
      def process(self,context):
          os.system('gsutil cp '+product_config['stagingbucket']+'/ojdbc6.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )
          logging.info('Jar copied to Instance..')
          logging.info('Java Libraries copied to Instance..')
          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')
          logging.info('Enviornment Variable set.')
          return list("1")
          

class readfromoracle(beam.DoFn): 
      def process(self, context, inpquery,targettable):
          database_user=env_config[connectionprefix+'_database_user']
#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')          database_password = env_config[connectionprefix + '_database_password']          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')
          database_host=env_config[connectionprefix+'_database_host']
          database_port=env_config[connectionprefix+'_database_port']
          database_db=env_config[connectionprefix+'_database']
          
          jclassname = "oracle.jdbc.driver.OracleDriver"
          url = ("jdbc:oracle:thin:"+database_user+"/"+database_password+"@"+database_host +":"+database_port+"/"+database_db)
          jars = ["/tmp/ojdbc6.jar"]
          libs = None
          cnx = jaydebeapi.connect(jclassname, url, jars=jars,
                            libs=libs)   
          logging.info('Connection Successful..') 
          cnx.cursor()
          logging.info('Reading Sql Query from the file...')
          query = inpquery.replace('v_incr_date',incrementaldate).replace('jobrunid',str(jobrunid)).replace('jobname', jobname).replace('v_input',input)
          logging.info('Query is %s',query)
          logging.info('Query submitted to Oracle Database..')

          for chunk in pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=100000):
                 chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists=writedeposition)
    
          logging.info("Load completed...")
          return list("1")
      
class runbqsql(beam.DoFn):
      def process(self, context, inputquery):
          client = bigquery.Client(project=env_config['projectid'])
          query=inputquery
          query_job = client.query(query)
          results = query_job.result()
          print(results)
          return list("1")     

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
                     '--service_account_key_file', env_config['service_account_key_file'],
                     '--worker_machine_type', "n1-standard-8",
                     '--extra_package', env_config['extra_package']
                     ]
    
    try:
        
        pcoll = beam.Pipeline(argv=pipeline_args)
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])
        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())
        (dummy_env | 'Complaint' >>  beam.ParDo(readfromoracle(),landing_cts_complaint_dim,'WORK_CTS_COMPLAINT_DIM')
        | 'Complaint Dim..' >>  beam.ParDo(runbqsql(),merge_cts_complaint_dim))
        (dummy_env | 'Complaint History' >>  beam.ParDo(readfromoracle(),landing_cts_complaint_hist_dim,'WORK_CTS_COMPLAINT_HISTORY_DIM')
        | 'Complaint History Dim..' >>  beam.ParDo(runbqsql(),merge_cts_complaint_hist_dim))
        (dummy_env | 'Incident' >>  beam.ParDo(readfromoracle(),landing_cts_incident_fact,'WORK_CTS_INCIDENT_FACT')
        | 'Incident Fact..' >>  beam.ParDo(runbqsql(),merge_cts_incident_fact))
        (dummy_env | 'Incident History' >>  beam.ParDo(readfromoracle(),landing_cts_incident_hist_fact,'WORK_CTS_INCIDENT_HISTORY_FACT')
        | 'Incident History Fact..' >>  beam.ParDo(runbqsql(),merge_cts_incident_hist_fact))
        (dummy_env | 'Incident Routing' >>  beam.ParDo(readfromoracle(),landing_cts_incident_routing_list_fact,'WORK_CTS_INCIDENT_ROUTING_LIST_FACT')
        | 'Incident Routing Fact..' >>  beam.ParDo(runbqsql(),merge_cts_incident_routing_list_fact))
        p=pcoll.run()
        p.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise    

def main(args_config,args_productconfig,args_env,args_connectionprefix,args_incrementaldate,args_deposition,args_input):
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
    global connectionprefix
    connectionprefix = args_connectionprefix
    global incrementaldate
    incrementaldate=args_incrementaldate
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    global sqlstring
    global writedeposition
    writedeposition=args_deposition
    global input
    input = args_input
    jobname="load-"+product_config['productname']+"-"+"landing"+"-"+time.strftime("%Y%m%d")
    logging.info('Job Name is %s',jobname)

    global landing_cts_complaint_dim
    global landing_cts_complaint_hist_dim
    global landing_cts_incident_fact
    global landing_cts_incident_hist_fact
    global landing_cts_incident_routing_list_fact
       
    landing_cts_complaint_dim=readfileasstring(cwd+'/sql/insert_work_cts_complaint_dim.sql')
    landing_cts_complaint_hist_dim=readfileasstring(cwd+'/sql/insert_work_cts_complaint_history_dim.sql')
    landing_cts_incident_fact=readfileasstring(cwd+'/sql/insert_work_cts_incident_fact.sql')
    landing_cts_incident_hist_fact=readfileasstring(cwd+'/sql/insert_work_cts_incident_history_fact.sql')
    landing_cts_incident_routing_list_fact=readfileasstring(cwd+'/sql/insert_work_cts_incident_routing_list_fact.sql')
    
    
    global merge_cts_complaint_dim
    global merge_cts_complaint_hist_dim
    global merge_cts_incident_fact
    global merge_cts_incident_hist_fact
    global merge_cts_incident_routing_list_fact
    
    merge_cts_complaint_dim=readfileasstring(cwd+'/sql/merge_cts_complaint_dim.sql').replace('v_job_run_id',str(jobrunid)).replace('v_job_name',jobname)
    merge_cts_complaint_hist_dim=readfileasstring(cwd+'/sql/merge_cts_complaint_history_dim.sql').replace('v_job_run_id',str(jobrunid)).replace('v_job_name',jobname)
    merge_cts_incident_fact=readfileasstring(cwd+'/sql/merge_cts_incident_fact.sql').replace('v_job_run_id',str(jobrunid)).replace('v_job_name',jobname)
    merge_cts_incident_hist_fact=readfileasstring(cwd+'/sql/merge_cts_incident_history_fact.sql').replace('v_job_run_id',str(jobrunid)).replace('v_job_name',jobname)
    merge_cts_incident_routing_list_fact=readfileasstring(cwd+'/sql/merge_cts_incident_routing_list_fact.sql').replace('v_job_run_id',str(jobrunid)).replace('v_job_name',jobname)
    
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
        '--connectionprefix',
        required=True,
        help= ('connectionprefix either Schema')
        )
    
    parser.add_argument(
        '--incrementaldate',
        required=True,
        help= ('Incremental Data Pull Filter date')
        )
    
    parser.add_argument(
        '--deposition',
        required=True,
        help= ('Append Or Truncate, append or replace ')
        )

    parser.add_argument(
        '--input',
        required=True,
        help= ('file to be loaded ')
        )
    args = parser.parse_args()   
        
    main(args.config,
         args.productconfig,
         args.env,
         args.connectionprefix,
         args.incrementaldate,
         args.deposition,
         args.input)    