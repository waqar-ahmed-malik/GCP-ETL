'''
Created on April 25, 2018

@author: Ramneek Kaur
This module reads SQL files to run queries on BigQuery.
This module takes Input,update or Select Query(sql File) and Sqlfile  with location as arguments. 
3. Other Arguments to this module is Config File, Product Config and enviornment.
'''
import argparse
import configparser
import argparse
import logging
from google.cloud import bigquery
 
def readfileasstring(sqlfile): 
      
     """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 
     with open (sqlfile, "r") as file:
         sqltext=file.read().strip("\n").strip("\r")
     return sqltext


def query_travel():
     client = bigquery.Client(project=env_config['projectid'])
     query_job = client.query(readfileasstring(product_config['sqlfilelocation']))
     results = query_job.result()
     print(results)

     for row in results:
         print((format(row.TRAVELER_EMAIL)))
         
def run():
    query_travel()

def main(args_config,args_productconfig,args_env,args_sqlfile):
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open('D://PROJECTS//AAA//cloud_sdk//GCPDWH//util//readconfig.py').read(), 'D://PROJECTS//AAA//cloud_sdk//GCPDWH//util//readconfig.py', 'exec'), globals())
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global sqlfile
    sqlfile= args_sqlfile   
    
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

# [END all]