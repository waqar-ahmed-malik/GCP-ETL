import argparse
import os
import configparser
import argparse
import logging
from google.cloud import bigquery

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
         print((format(row)))

def main(args_config,args_productconfig,args_env,args_sqlfile):
    logging.getLogger().setLevel(logging.INFO)
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open("C:\\cloud_sdk\\GCPDWH\\util\\readconfig.py").read(), "C:\\cloud_sdk\\GCPDWH\\util\\readconfig.py", 'exec'), globals()) 
    env_config =readconfig(config,env)
    global product_config
    product_config = readconfig(productconfig,env)
    global sqlfile
    sqlfile= args_sqlfile
    cwd = os.path.dirname(os.path.abspath(__file__))   
    global sqlfilepath
    sqlfilepath=cwd+"\\"+product_config['productname']+"\\sql\\"+sqlfile
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']    
    logging.info('Reading file from File...')
    sql=readfileasstring(sqlfilepath).split(';')
    
    for inputsql in sql:
        logging.info('Reading Query....')
        logging.info('Executing query...%s',inputsql)
        run_query(inputsql)  

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
