'''

modified on march 25, 2019

@modified: Ramneek kaur

'''



 

import apache_beam as beam

import time

import os

import argparse

from google.cloud import bigquery

import logging

import sys

from google.cloud import storage as gstorage

import pandas

from oauth2client.client import GoogleCredentials



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

                     '--requirements_file', '/home/airflow/gcs/data/GCPDWH/workday/requirements.txt',

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

                     '--worker_machine_type', "n1-standard-2",

                    ]

   

    

    try:

       

        pcoll = beam.Pipeline(argv=pipeline_args)

        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])

          

    

        (dummy | 'land_travel_globalware_transactions' >>  beam.ParDo(runbqsql(),land_travel_globalware_transactions)

        | 'merge_travel_globalware_transactions' >>  beam.ParDo(runbqsql(),merge_travel_globalware_transactions))

        (dummy | 'land_travel_tst_transactions' >>  beam.ParDo(runbqsql(),land_travel_tst_transactions)

        | 'merge_travel_tst_transactions' >>  beam.ParDo(runbqsql(),merge_travel_tst_transactions))

     

        

        p = pcoll.run()

        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise    



def main(args_config,args_productconfig,args_env):

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

    global sqlstring

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)

    global jobname

    jobname="load-"+product_config['productname']+"-"+"landing"+"-"+time.strftime("%Y%m%d")

    logging.info('Job Name is %s',jobname)

    

    global land_travel_globalware_transactions

    global land_travel_tst_transactions

    global merge_travel_globalware_transactions

    global merge_travel_tst_transactions

   

    

    land_travel_globalware_transactions = readfileasstring(cwd + '/sql/work_travel_globalware_transactions.sql')

    land_travel_tst_transactions = readfileasstring(cwd + '/sql/work_travel_tst_transactions.sql')

    merge_travel_globalware_transactions = readfileasstring(cwd+'/sql/merge_travel_globalware_transactions.sql')

    merge_travel_tst_transactions = readfileasstring(cwd+ '/sql/merge_travel_tst_transactions.sql')

    

    

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



    args = parser.parse_args()   

        

    main(args.config,

         args.productconfig,

         args.env)    
