'''

Created on Aug 03, 2018

This script loads all the mart integration tables for Insurance.

Tables Loaded from this Script are :

1. Insurance Work Dim - Landing Dataset

2. Insurance Dim - Customer Product Dataset

3. Insurance Customer Dim - Customers Dataset

3. Insurance Transaction  fact - Customer Product Dataset 

4. 

@author: Rajnikant Rakesh

'''



from datetime import datetime, timedelta

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"



def readfileasstring(sqlfile):     

     """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 

     with open (sqlfile, "r") as file:

         sqltext=file.read().strip("\n").strip("\r")

     return sqltext



def run_query(inputquery):

     client = bigquery.Client(project=env_config['projectid'])

     query=inputquery

     query_job = client.query(query)

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

    

    '''Load Insurance Transaction Fact  Table - insurance_transaction_fact '''

    logging.info('Loading INSURANCE TRANSACTIONS FACT table ....')    

    run_query(readfileasstring(cwd+"/sql/insurance_transaction_fact.sql").replace('v_job_run_id',str(jobrunid)))

    

    

    '''Load Insurance Transaction Fact update Agent - insurance_transaction_fact '''

#     logging.info('Update Agent in  INSURANCE TRANSACTIONS FACT table ....')    

#     run_query(readfileasstring(cwd+"/sql/insurance_transaction_fact_agent_update.sql").replace('v_job_run_id',str(jobrunid)))

   

    ''' Move files to Archive folder '''

    '''

    logging.info("Archiving files...")

    movefiles= "gsutil mv gs://"+product_config['bucket']+"/current/*"+" gs://"+product_config['bucket']+"/archive/"

    print movefiles

    os.system(movefiles)

    '''

    

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
