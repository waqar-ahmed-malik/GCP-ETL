'''

Created on June 7 ,2019

This script loads all the mart integration tables for Insurance.

Tables Loaded from this Script are :

1. Insurance Work Policy Dim - Landing Dataset

2. Insurance policy Dim - Customer Product Dataset

3. Insurance Unit Dim - Customers Dataset

3. Insurance Transaction Dim  - Customer Product Dataset

4. MDM integration

@author: Ramneek Kaur

'''



import apache_beam as beam

import argparse

import logging

import os

import sys

import time

from google.cloud import bigquery

from googleapiclient import discovery

from oauth2client.client import GoogleCredentials

from datetime import datetime, timedelta

from google.cloud import storage



cwd = os.path.dirname(os.path.abspath(__file__))



if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    mdmpath = cwd[:folderup]+"\\mdm\\"

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    mdmpath = cwd[:folderup]+"/mdm/"

    

def readfileasstring(sqlfile):

    """Read any text file and return text as a string. This function is used to read .sql and .schema files"""

    with open (sqlfile, "r") as file:

        sqltext=file.read().strip("\n").strip("\r")

    return sqltext

 

class runbqsql(beam.DoFn):

    def process(self, context, inputquery):

          client = bigquery.Client(project=env_config['projectid'])

          query=inputquery

          query_job = client.query(query)

          results = query_job.result()

          print(results)

          return list("1")  

 

class runbqsqloop(beam.DoFn):

    def process(self, context, inputquery):

          client = bigquery.Client(project=env_config['projectid'])

          inputquerysplit=inputquery.split(';')

          for inputsql in inputquerysplit:

            query=inputsql

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

        

        work_insurance_policy = (dummy | 'LOAD WORK INSURANCE POLICY DIM ' >>  beam.ParDo(runbqsql(), landing_work_insurance_policy))

        md5_work_insurance_policy = (work_insurance_policy | 'MD5 UDPATE WORK INSURANCE POLICY DIM' >>  beam.ParDo(runbqsql(), update_work_insurance_policy))

        mdm_insurance_work = (md5_work_insurance_policy | 'MDM WORK TABLE LOAD ' >>  beam.ParDo(runbqsqloop(), load_mdm_insurance_work))

        mdm_insurance_load = (mdm_insurance_work | 'MDM CUSTOMER DIM FOR INSURANCE LOAD ' >>  beam.ParDo(runbqsqloop(), load_mdm_insurance))

        load_insurance_policy = (mdm_insurance_load | 'LOAD INSURANCE POLICY DIM ' >>  beam.ParDo(runbqsql(), load_insurance_policy_dim))

        update_insurance_polciy = (load_insurance_policy | 'UPDATE INSURANCE POLICY DIM ' >>  beam.ParDo(runbqsql(), update_insurance_polciy_dim))

        (update_insurance_polciy | 'LOAD INSURANCE UNIT DIM ' >>  beam.ParDo(runbqsql(), load_insurance_unit_dim))

        (update_insurance_polciy | 'LOAD  INSURANCE TRANSACTION DIM ' >>  beam.ParDo(runbqsql(), load_insurance_transaction_dim)

        | 'LOAD INSURANCE CUSTOMER DIM ' >>  beam.ParDo(runbqsql(), load_insurance_customer_dim)

        | 'LOAD INSURANCE COMPLAINCE ' >>  beam.ParDo(runbqsql(), load_insurance_compliance_audit))

     



        



        p = pcoll.run()

        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise



def main(config, productconfig, env):

    """

    1. Define required global variables.

    2. Set Deposition method to WRITE_TRUNCATE in case not specified tough it is required parameter.

    3. Generate stream for Audit fields in data. i.e. BQ_CREATED_BY= {DataFlow Job Name} and BQ_CREATE_TS= {Dataflow Job Start Time}

    4. Read config.properties by calling readconfig method from readconfig.py file

    5. Authenticate GCP project, read json file from config file. (This is explict authentication)

    6. Read BigQuery Table schema using readshcema method from readschema.py file.

    7. Call DataFlow PipeLine method, this initiate Dataflow job on GCP.

    8. Call wait_for_dataflow_job_finish method to monitor the load till it finishes.

    """

    global save_main_session

    global datasetname

    global job_timestamp

    global jobname

    global projectid

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)



    ts = time.gmtime()

    job_timestamp = time.strftime("%Y%m%d", ts)

    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())

    global env_config

    env_config = readconfig(config, env )



    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']



    global product_config

    product_config = readconfig(productconfig, env)

    projectid=env_config['projectid']

    exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals())

    datasetname=product_config['datasetname']

    jobname = "load-" + product_config['productname']+"-dwh-mart-" +job_timestamp



    print(jobname)



    global landing_work_insurance_policy

    global update_work_insurance_policy

    global load_mdm_insurance_work

    global load_mdm_insurance

    global load_insurance_policy_dim

    global update_insurance_polciy_dim

    global load_insurance_unit_dim

    global load_insurance_transaction_dim 

    global load_insurance_customer_dim 

    global load_insurance_compliance_audit

    

    

    

    landing_work_insurance_policy = readfileasstring(cwd+"/sql/insurance_policy_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)

    update_work_insurance_policy = readfileasstring(cwd+"/sql/work_insurance_policy_dim_MD5_update2.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)

    load_mdm_insurance_work = readfileasstring(mdmpath+"/ivans/load_mdm_stg.sql")

    load_mdm_insurance = readfileasstring(mdmpath+"/ivans/mdm_processing.sql")

    load_insurance_policy_dim = readfileasstring(cwd+"/sql/insurance_policy_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)

    update_insurance_polciy_dim = readfileasstring(cwd+"/sql/insurance_policy_dim_update_date.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)

    load_insurance_unit_dim = readfileasstring(cwd+"/sql/insurance_unit_detail_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)

    load_insurance_transaction_dim = readfileasstring(cwd+"/sql/insurance_transaction_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname) 

    load_insurance_customer_dim = readfileasstring(cwd+"/sql/insurance_customer_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)

    load_insurance_compliance_audit = readfileasstring(cwd+"/sql/insurance_compliance_audit.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)



  



   

    run()

    sys.exit()



if __name__ == '__main__':

    """Input -> Config file,

    product config file,

    env

    """

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(

        '--config',

        required=True,

        help= ('Config file name with full path of file')

        )

    parser.add_argument(

        '--productconfig',

        required=True,

        help= ('Product config file name with full path of file')

        )

    parser.add_argument(

        '--env',

        required=True,

        help= ('Enviornment to be run dev/test/prod')

        )





    args = parser.parse_args()



    main(args.config,

         args.productconfig,

         args.env

         )
