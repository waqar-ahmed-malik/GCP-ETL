'''
Created on June 10, 2019
@author: Prerna Anand
'''
import argparse
import configparser
import logging
from google.cloud import bigquery
import os
from string import lower
from fileinput import filename

cwd = os.path.dirname(os.path.abspath(__file__)) 

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    homepath = cwd[:folderup]+"\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
    homepath = cwd[:folderup]+"/"
     
def readfileasstring(sqlfile): 
      
     """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 
     with open (sqlfile, "r") as file:
         sqltext=file.read().strip("\n").strip("\r")
     return sqltext

def run_query():
     client = bigquery.Client(project=env_config['projectid'])
     query_job = client.query(readfileasstring(os.path.abspath(homepath+product_config['productname']+'/sql/'+sqlfile)).replace("v_job_run_id","").replace("v_job_name","load-external-payments").replace("v_file_name",filename).replace("v_file_date",filedate))
     results = query_job.result()
     print(results)


def run():
    run_query()

def main(args_config,args_productconfig,args_env,args_sqlfile,args_tablename,args_filename,args_filedate):
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
    global tablename  
    tablename = args_tablename
    global filename
    filename=args_filename
    global filedate
    filedate=args_filedate
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
    parser.add_argument(
        '--tablename',
        required=True,
        help= ('Input Query table name')
        )  
    parser.add_argument(
        '--filename',
        required=True,
        help= ('Source file name')
        )  
    parser.add_argument(
        '--filedate',
        required=True,
        help= ('date present in source file name')
        )
  
    args = parser.parse_args()   
    
    
    main(args.config,
         args.productconfig,
         args.env,
         args.sqlfile,
         args.tablename,
         args.filename,
         args.filedate
         )
