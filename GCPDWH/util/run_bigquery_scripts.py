'''
Created on May 25, 2018

@author: Ramneek Kaur
This module reads SQL files to run queries on BigQuery.
This module takes Input,update or Select Query(sql File) and Sqlfile  with location as arguments. 
Other Arguments to this module is Config File, Product Config and enviornment.

Revision on July 3, 2018
@author: Rajnikant Rakesh
Changed util directory alongwith logging and exception
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


def run_query():
     client = bigquery.Client(project=env_config['projectid'])
     logging.info('Reading Query...')
     query=readfileasstring(sqlfile)
     logging.info('Query is %s',query)
     query_job = client.query(query)
     results = query_job.result()
     print(results)
     for row in results:
         print((format(row)))
         

def main(args_config,args_productconfig,args_env,args_sqlfile):
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals()) 
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global sqlfile
    sqlfile= args_sqlfile   
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
    
    run_query()  
           
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

# [END all]