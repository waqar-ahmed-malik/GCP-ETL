'''

#Created on Feb 06, 2019

This script loads all the mart integration tables for Insurance.

Tables Loaded from this Script are :

1. Insurance Work Policy Dim - Landing Dataset

2. Insurance policy Dim - Customer Product Dataset

3. Insurance Unit Dim - Customers Dataset

3. Insurance Transaction Dim  - Customer Product Dataset 

4. 

@author: Ramneek Kaur

#Modified Feb 11, 2019

Added MDM logic to the script

@author: Atul Guleria

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

    #Else go with linux path

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

    os.listdir(cwd+'/sql/')  

    #print product_config['bucket']

    current_date=(datetime.today() - timedelta(days=1)).strftime("%m.%d.%Y")

    print(current_date)

    

   

    '''Load Work Insurance Table - insurance_work_policy_dim '''

    logging.info('Loading insurance_work_policy_dim table ....')   

    run_query(readfileasstring(cwd+"/sql/work_insurance_policy_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))

    

    '''Update MD5 Values in insurance work dim'''

    logging.info('Updating insurance_work_polic_dim MD5 table ....') 

    run_query(readfileasstring(cwd+"/sql/work_insurance_policy_dim_MD5_update2.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)) 

#Adding MDM logic here    

    '''Update MDM tables with new data'''

    logging.info('Updating MDM tables ....') 

    stg_sql=readfileasstring(mdmpath+"/ivans/load_mdm_stg.sql").split(';')

    mdm_sql=readfileasstring(mdmpath+"/ivans/mdm_processing.sql").split(';')

    for inputsql in stg_sql:

        run_query(inputsql) 

    logging.info('MDM_WORK_IVANS_IE_STG load finished...')     

    for inputsql in mdm_sql:

        run_query(inputsql) 

    logging.info('MDM process finished...') 



    '''Load Insurance Dim Table - insurance_policy_dim '''

    logging.info('Loading Insurance Policy Dim table..') 

    run_query(readfileasstring(cwd+"/sql/insurance_policy_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)) 

    

    '''Update type 2 Insurance Policy Dim Table - insurance_policy_dim '''

    logging.info('Updating insurance_dim  table ....')    

    run_query(readfileasstring(cwd+"/sql/insurance_policy_dim_update_date.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)) 

    

#     '''Truncnate insurance unit dim detail table '''

#     logging.info('truncate insurance_unit_detail_dim  table ....')    

#     run_query(readfileasstring(cwd+"/sql/insurance_unit_dim_detail_truncate.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname)) 

    

    '''Insert into  insurance unit dim detail table '''

    logging.info('Insert into  insurance unit dim detail table ....')    

    run_query(readfileasstring(cwd+"/sql/insurance_unit_detail_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))

    

    

    '''Insert into  insurance transaction dim  table '''

    logging.info('Insert into  insurance transaction dim  table ....')    

    run_query(readfileasstring(cwd+"/sql/insurance_transaction_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))

    

    

    '''Insert into  insurance customer dim  table '''

    logging.info('Insert into  insurance transaction dim  table ....')    

    run_query(readfileasstring(cwd+"/sql/insurance_customer_dim.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))

    

    '''Insert into  insurance compliance Audit table '''

    logging.info('Insert into  insurance transaction dim  table ....')    

    run_query(readfileasstring(cwd+"/sql/insurance_compliance_audit.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))   

    

    

    

    

    logging.info("Insurance Loading process completed..")

              

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
