'''
Created on Jan 01, 2019
@author: Atul Guleria
This module processes MDM data
'''

import argparse
import os
import configparser
import argparse
import logging
from google.cloud import bigquery
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
     query_job.result()
     return 1

def run():

    
    sql=readfileasstring(sqlfilepath).split(';')
    
    for inputsql in sql:
        logging.info('Executing query...%s',inputsql)
        run_query(inputsql) 
        logging.info('Query completed!...')
    logging.info('Work tables truncated...')    
    
def main(args_config,args_productconfig,args_env,args_sqlfile):
    logging.getLogger().setLevel(logging.INFO)
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(utilpath+'readconfig.py').read(), utilpath+'readconfig.py', 'exec'), globals())
    env_config =readconfig(config,env)
    global product_config
    product_config = readconfig(productconfig,env)
    global sqlfile
    sqlfile= args_sqlfile
    cwd = os.path.dirname(os.path.abspath(__file__))   
    global sqlfilepath
    sqlfilepath=cwd+"/"+sqlfile
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
        '--sqlfile',
        required=True,
        help= ('Input Query file location')
        )
  
    args = parser.parse_args()   
    
    main(args.config,
         args.productconfig,
         args.env,
         args.sqlfile
         )
