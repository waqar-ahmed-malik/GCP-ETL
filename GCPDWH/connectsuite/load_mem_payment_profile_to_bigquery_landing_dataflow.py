'''
Created on May 15,2019
@author: Ramneek Kaur
This module reads data from Sqlserver and Write into Bigquery Table
It uses pandas dataframe to read and write data to BigQuery.
'''

 
import apache_beam as beam
import time
import jaydebeapi 
import os
import argparse
from google.cloud import bigquery
import logging
import sysimport base64
from google.cloud import storage as gstorage
import pandas
from oauth2client.client import GoogleCredentials

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

def readfileasstring(sqlfile):   
    """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 
    with open (sqlfile, "r") as file:
        sqltext=file.read().strip("\n").strip("\r")
    return sqltext


class setenv(beam.DoFn): 
      def process(self,context):
          os.system('gsutil cp '+product_config['stagingbucket']+'/sqljdbc41.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )
          logging.info('Jar copied to Instance..')
          logging.info('Java Libraries copied to Instance..')
          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')
          logging.info('Enviornment Variable set.')
          return list("1")
      
      
class readfromssql(beam.DoFn): 
      def process(self, context, inquery,targettable):
          database_user=env_config[connectionprefix+'_database_user']
#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')          database_password = env_config[connectionprefix + '_database_password']          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')
          database_host=env_config[connectionprefix+'_database_host']
          database_database=env_config[connectionprefix+'_database']
          database_port=env_config[connectionprefix+'_database_port']
          
          jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
          url = ("jdbc:sqlserver://" + database_host + ':' + database_port + ';database=' + database_database + ';user=' + database_user + ';password=' + database_password)
          jars = ["/tmp/sqljdbc41.jar"]
          libs = None
          cnx = jaydebeapi.connect(jclassname, url, jars=jars,
                            libs=libs)   
          logging.info('Connection Successful..')
          cnx.cursor()
          logging.info('Reading Sql Query from the file...')
          query = inquery.replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
          logging.info('Query is %s',query)
          logging.info('Query submitted to SqlServer Database')

          logging.info("Started loading Table")
          for chunk in pandas.read_sql(query.replace("v_database_name", database_database), cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=500000):
              chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='append')
             
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
        
        
   
        
        #newly added tables on 5/10/19
        
        (dummy_env | 'DELETE WORK PAYMENT PROFILE' >>  beam.ParDo(runbqsql(),delete_e_payment_profile)
        | 'WORK E PAYMENT PROFILE' >>  beam.ParDo(readfromssql(),landing_cs_e_payment_profile ,'WORK_CS_E_PAYMENT_PROFILE')
        | 'E PAYMENT PROFILE' >>  beam.ParDo(runbqsql(),cs_e_payment_profile))
        (dummy_env | 'DELETE WORK MBRSHIP E PAYMENT PROFILE' >>  beam.ParDo(runbqsql(),delete_mbrship_e_payment_profile)
        | 'WORK MEMBERSHIP E PAYMENT PROFILE' >>  beam.ParDo(readfromssql(),landing_cs_mbrship_e_payment_profile ,'WORK_CS_MBRSHIP_E_PAYMENT_PROFILE')
        | 'MEMBERSHIP E PAYMENT PROFILE' >>  beam.ParDo(runbqsql(),cs_mbrship_e_payment_profile))
        
        p = pcoll.run()
        p.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise    

def main(args_config,args_productconfig,args_env,args_connectionprefix,args_inputdate):
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
    global sqlstring
    global jobrunid
    jobrunid=os.getpid()
    global inputdate
    inputdate=args_inputdate
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    jobname="load-"+product_config['productname']+"-"+"land-mem-payment-profile"+"-"+time.strftime("%Y%m%d")
    logging.info('Job Name is %s',jobname)
    
    
    global delete_e_payment_profile
    global delete_mbrship_e_payment_profile   
    
    global landing_cs_e_payment_profile
    global landing_cs_mbrship_e_payment_profile
    
    
    global cs_e_payment_profile
    global cs_mbrship_e_payment_profile   
    
    
    delete_e_payment_profile = readfileasstring(cwd + '/sql/mem_payments/delete_work_e_payment_profile.sql')
    delete_mbrship_e_payment_profile = readfileasstring(cwd + '/sql/mem_payments/delete_work_membrship_e_payment_profile.sql')
 
    
    landing_cs_e_payment_profile = readfileasstring(cwd + '/sql/mem_payments/work_cs_e_payment_profile.sql')
    landing_cs_mbrship_e_payment_profile = readfileasstring(cwd + '/sql/mem_payments/work_cs_mbrship_e_payment_profile.sql')
    
    
    
    cs_e_payment_profile = readfileasstring(cwd + '/sql/mem_payments/connectsuite_e_payment_profile.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    cs_mbrship_e_payment_profile = readfileasstring(cwd + '/sql/mem_payments/connectsuite_mbrship_e_payment_profile.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    
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
        '--inputdate',
        required=True,
        help= ('airflow execution date')
        )
        
    args = parser.parse_args()   
        
    main(args.config,
         args.productconfig,
         args.env,
         args.connectionprefix,
         args.inputdate
        )    