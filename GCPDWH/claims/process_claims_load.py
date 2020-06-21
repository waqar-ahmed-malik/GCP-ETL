'''

Created on July 3, 2018

@author: Rajnikant Rakesh





modified on feb 8,2019

@author : Ramneek kaur

deleteion of insurance claims customer dim , modifed insurance claims dim, added update dlete and new insuert query for insurance lcaims dim .



'''

 

import time

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

from datetime import datetime, timedelta

import logging



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



def run_query(inputquery):

     client = bigquery.Client(project=env_config['projectid'])

     query=inputquery

     query_job = client.query(query)

     results = query_job.result()

     print(results)

     return list("1")    



def main(args_config,args_productconfig,args_env,args_input):

    logging.getLogger().setLevel(logging.INFO)

    global input

    input = args_input

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

    jobname= "load-insurance-claims"

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]



    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    

    logging.info('Started Loading Files  ....')



    for blob in bucket.list_blobs(prefix=input):

        filename=blob.name

        print(filename)

        file="gs://"+product_config['bucket']+"/"+filename

        loadtables='python '+cwd+'/load_claims_to_bigquery_landing.py --config config.properties --productconfig claims.properties --env prod  --input '+ file +' --separator "|" --stripheader 1 --stripdelim 0  --addaudit 1  --output WORK_INSURANCE_CLAIMS   --writeDeposition WRITE_TRUNCATE'

        print(loadtables)

        os.system(loadtables)

    

    

    '''update Claim Dim Tables - Insurance Claims  Dim'''

    logging.info('update INSURANCE_CLAIMS_CUSTOMER_DIM table ....')   

    run_query(readfileasstring(cwd+"/sql/claims_dim_update.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))

    

    

    '''delete Claim Dim Tables - Insurance Claims  Dim'''

    logging.info('delete INSURANCE_CLAIMS_CUSTOMER_DIM table ....')   

    run_query(readfileasstring(cwd+"/sql/claims_dim_delete.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))

    

    

    

    '''insert Claim Dim Tables - Insurance Claims  Dim'''

    logging.info('insert INSURANCE_CLAIMS_CUSTOMER_DIM table ....')   

    run_query(readfileasstring(cwd+"/sql/claims_dim_insert.sql").replace ('V_JOB_RUN_ID',str(jobrunid)).replace ('V_JOB_NAME',jobname))

    

        

    logging.info("Claims Loading process completed..")

              

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

        '--input',

        required=True,

        help= ('filename')

        ) 

        

    args = parser.parse_args()       

 

main(args.config,

     args.productconfig,

     args.env,

     args.input)              
