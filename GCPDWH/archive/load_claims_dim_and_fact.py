'''
Created on August 03, 2018
This modules loads the claims Dimension and Fact tables from claims work table in landing dataset
The sql's in the same folder then transfers the data into Insurance fact and Insurance customer tables.
@author: Rajnikant Rakesh

Arguments Required : 

'''

import argparse
import logging
import os
import sys
from google.cloud import bigquery
from string import lower
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time

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
    for row in results:
        dummy=1

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
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    jobname="load_"+product_config['productname']
    logging.info('Job Name is %s',jobname)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]
    sqlfile=os.listdir(cwd+'\\sql\\')    
    
    logging.info('Loading Insurance Claims Dim table ...')   
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[0]).replace('V_JOB_RUN_ID',str(jobrunid)).replace('V_JOB_NAME',str(jobname)))
    
    logging.info('Loading Insurance Claims Fact table ....') 
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[1]).replace('V_JOB_RUN_ID',str(jobrunid)).replace('V_JOB_NAME',str(jobname)))          

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