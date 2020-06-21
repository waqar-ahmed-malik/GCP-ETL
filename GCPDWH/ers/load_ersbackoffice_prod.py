'''
#Created on Jun 07, 2019
This script loads ERS tables in a different project from current ers tables
@author: Atul Guleria
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
    erspath = cwd[:folderup]+"\\ers\\sql\\ers_back_office\\"
    
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
    erspath = cwd[:folderup]+"/ers/sql/ers_back_office/"
    
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
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]
    
    logging.info('Started Loading ers tables in aaa-mwg-ersbackofficeprod....') 
    dup_call_lst=readfileasstring(erspath+"create_ersbackoffice_dup_call_prod.sql")
    run_query(dup_call_lst)          
    sc_call_sql_file=readfileasstring(erspath+"create_ersbackoffice_sc_call_comment_prod.sql")
    run_query(sc_call_sql_file) 
    service_call_sql_file=readfileasstring(erspath+"create_ersbackoffice_service_call_details_prod.sql")
    run_query(service_call_sql_file) 
    logging.info('ERS tables loaded in aaa-mwg-ersbackofficeprod')     
              
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