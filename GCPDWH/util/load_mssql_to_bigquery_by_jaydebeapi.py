import jaydebeapi
import pandas
import datetime
import os
import argparse
import csv
from google.cloud import bigquery
import logging
import sys
import pymssql
from string import lower
sys.path.append(os.path.abspath('..\deployment'))
from google.cloud import storage as gstorage
from google.cloud.storage import blob as Blob
from googleapiclient import discovery
import pandas
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
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

def run():
    
    database_user=env_config[connectionprefix+'_database_user']
    database_password=env_config[connectionprefix+'_database_password']
    database_host=env_config[connectionprefix+'_database_host']
    database_database=env_config[connectionprefix+'_database']
    
    jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = ("jdbc:sqlserver://" + database_host + ';database=' + database_database + ';user=' + database_user + ';password=' + database_password)
    jars = ["C:/GCP Projects/sqljdbc_6.0.8112.200_enu/sqljdbc_6.0/enu/jre7/sqljdbc41.jar"]
    libs = None
    cnx = jaydebeapi.connect(jclassname, url, jars=jars,
                            libs=libs)   
#     cnx = pymssql.connect(server=database_host, user=database_user,password=database_password,database=database_database)
    cnx.cursor()
    logging.info('Reading Sql Query from the file...')
    #reader = pandas.read_csv(incrementaldate, delimiter=',')
    #for column in reader:
#     query = "select top 3 * from mbr"
    query = readfileasstring(sqlfile).replace('v_incr_date',incrementaldate)
    logging.info('Query is %s',query)
    logging.info('Query submitted to SqlServer Database')
    #queryresult=pandas.read_sql(query, cnx, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None)
    #print queryresult
    #queryresult['JOB_RUN_ID']= jobrunid
    #queryresult['CREATED_BY']=jobname
    #queryresult['SOURCE_SYSTEM_CD']=product_config['productname']
    #rows=pandas.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None)
    #rows.dtypes
    logging.info("Started loading Table")
    for chunk in pandas.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=100000):
        chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='replace')
    '''
    queryresult.to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='append')
    logging.info("Load completed")
    #,if_exists='append'
   '''    
    logging.info("Load completed...")

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
    #os.path.abspath('../'+'mssql'+'/'+'schema/'+args_incrementaldate)
    #incrementaldate=os.path.abspath("././")
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