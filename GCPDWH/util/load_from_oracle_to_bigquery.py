'''
Created on June 27, 2018
@author: Rajnikant Rakesh
This module reads data from Oracle and Write into Bigquery Table
It uses pandas dataframe to read and write data to BigQuery.
'''


# import jaydebeapi 
import datetime
import cx_Oracle
import os
import argparse
from google.cloud import bigquery
import logging
import sys
from string import lower
from google.cloud import storage as gstorage
import pandas as  pd
from oauth2client.client import GoogleCredentials
import pytz

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    sqlpath = cwd[:folderup]+"\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
    sqlpath = cwd[:folderup]+"/"

print(utilpath)

def readfileasstring(sqlfile):   
    """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 
    with open (sqlfile, "r") as file:
        sqltext=file.read().strip("\n").strip("\r")
    return sqltext

def run():
    
    database_user=env_config[connectionprefix+'_database_user']
    database_password=env_config[connectionprefix+'_database_password']
    database_host=env_config[connectionprefix+'_database_host']
    
    cnx = cx_Oracle.connect(database_user+"/"+database_password.decode('base64')+"@"+database_host)
    cnx.cursor()
    logging.info('Reading Sql Query from the file...')
    query = readfileasstring(sqlpath+product_config['productname']+'/sql/'+sqlfile).replace('v_incr_date',incrementaldate).replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    logging.info('Query is %s',query)
    logging.info('Query submitted to Oracle Database')
    #queryresult=pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None )
    #print queryresult.dtypes
    for chunk in pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=100000):
        chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='append')
    
    '''
    queryresult['JOB_RUN_ID']= jobrunid
    queryresult['CREATED_BY']=jobname
    queryresult['SOURCE_SYSTEM_CD']=product_config['productname']
    logging.info("Started loading Table")
    queryresult.to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='append')
    '''
    logging.info("Load completed...")
    #,if_exists='append'


def main(args_config,args_productconfig,args_env,args_targettable,args_sqlquery,args_connectionprefix,args_incrementaldate):
    logging.getLogger().setLevel(logging.INFO)
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(utilpath+'readconfig.py').read(), utilpath+'readconfig.py', 'exec'), globals())
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global targettable
    targettable = args_targettable
    global sqlfile
    sqlfile = args_sqlquery
    global connectionprefix
    connectionprefix = args_connectionprefix
    global incrementaldate
    incrementaldate=args_incrementaldate
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    jobname="load_"+product_config['productname']+"_"+lower(targettable)
    logging.info('Job Name is %s',jobname)
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
        '--targettable',
        required=True,
        help= ('targettable to which data would be loaded')
        )
    
    parser.add_argument(
        '--sqlquery',
        required=True,
        help= ('sqlquery from which data needs to be extracted')
        )
         
    parser.add_argument(
        '--connectionprefix',
        required=True,
        help= ('connectionprefix either Schema')
        )
    
    parser.add_argument(
        '--incrementaldate',
        required=True,
        help= ('Incremental Data Pull Filter date')
        )

    args = parser.parse_args()   
        
    main(args.config,
         args.productconfig,
         args.env,
         args.targettable,
         args.sqlquery,
         args.connectionprefix,
         args.incrementaldate)    