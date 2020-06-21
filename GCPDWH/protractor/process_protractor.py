'''

Created on May 22,2019

This script loads all the protractor tables .

Tables Loaded from this Script are :

1. work protractor stage in landing dataset

2. COR FACT table  - Customer Product Dataset

3. COR MDM TABLES LOAD 



@author: Ramneek Kaur

'''



from datetime import datetime, timedelta

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging

import time

import sys



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



def run_query(inputquery):

     client = bigquery.Client(project=env_config['projectid'])

     query=inputquery

     query_job = client.query(query)

     if query_job.state == 'RUNNING':

        time.sleep(2)

     elif query_job.state == 'FAILURE':

        sys.exit("QUERY failed")        

     results = query_job.result()

     print(results)

     return list("1")  

         

def main(args_config,args_productconfig,args_env):

    logging.getLogger().setLevel(logging.INFO)

    global env

    env = args_env

    global config

    config= args_config

    global productconfig

    productconfig = args_productconfig

    global env_config

    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals()) 

    env_config =readconfig(config,env)

    global product_config

    product_config = readconfig(productconfig,env)

    global fileid

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)

    global jobname

    jobname="load-"+product_config['productname']

    logging.info('Job Name is %s',jobname)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]

     

   

    current_date=(datetime.today() - timedelta(days=1)).strftime("%m.%d.%Y")

    print(current_date) 

    

    

    '''Update MDM tables with new data for COR'''

    logging.info('Updating MDM tables ....') 

    stg_sql=readfileasstring(mdmpath+"/protractor/WORK_MDM_PROTRACTOR.sql").split(';')

    mdm_sql=readfileasstring(mdmpath+"/protractor/protractor_mdm_processing_concated.sql").split(';')

    for inputsql in stg_sql:

        run_query(inputsql) 

    logging.info('MDM_WORK_COR_STG load finished...')     

    for inputsql in mdm_sql:

        run_query(inputsql) 

    logging.info('MDM process finished...') 

    

    

    '''Load Protractor work stage table'''

    logging.info('Loading Protractor work stage table')    

    run_query(readfileasstring(cwd+"/sql/WORK_PROTRACTOR_STAGE_INVOICE.sql").replace('v_job_run_id',str(jobrunid)))

    logging.info('Loading Protractor work stage table complete')   

    

    

    '''Load Protractor work stage table'''

    logging.info('Loading COR Protractor TRANSACTION FACT ')    

    run_query(readfileasstring(cwd+"/sql/COR_TRANSACTION_FACT.sql").replace('v_job_run_id',str(jobrunid)))

    logging.info('Loading COR Protractor TRANSACTION FACT complete')   



    

    logging.info("Protractor  Loading process completed..")

              

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
