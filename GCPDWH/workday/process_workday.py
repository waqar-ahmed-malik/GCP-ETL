'''
Created on July 3, 2018
This script process all Workday files and loads Employee Type 2 dimension along with location dim.
Step 1 : Lands all the files into respective landing tables.
Step 2 : Loads Current workday table and Employee Type 2 dimension 
Step 3 : Archives processed files for archive folder  
@author: Rajnikant Rakesh
'''
from datetime import datetime, timedelta
import os
import argparse
from string import  upper
from google.cloud import bigquery
from google.cloud import storage
from string import upper
import logging

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
     results = query_job.result()
     print(results)
     return list("1")  
'''     
def run(sqlfilepath):
    sql=readfileasstring(sqlfilepath).split(';')
    for inputsql in sql:
        logging.info('Reading Query....')
        logging.info('Executing query...%s',inputsql)
        run_query(inputsql)
'''
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
    global fileid
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    jobname="load_"+product_config['productname']+"_"+"employee_dim"
    logging.info('Job Name is %s',jobname)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]
    sqlfile=os.listdir(cwd+'\\sql\\')  
    #print product_config['bucket']
    current_date=(datetime.today() - timedelta(days=1)).strftime("%m.%d.%Y")
    print(current_date)
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket(product_config['bucket'])
    logging.info('Started Loading Files  ....')
    for blob in bucket.list_blobs(prefix='current/wd-'):
        if blob.name.endswith(current_date+".csv"):
            filename=blob.name#.split('/')[1]
            targettable=upper(filename.split('/')[1].split('.')[0].replace('wd','WORKDAY').replace('-','_'))
            fileid=filename.split('.')  
            del fileid[0]
            del fileid[3]
            fileids = [ fileid[2],fileid[0],fileid[1] ]
            fileid=''.join(fileids)
            logging.info('Loading %s file into %s table ',filename.split('/')[1],targettable)
            loadtables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig workday.properties --env prod --targettable " + targettable + " --filename "+ filename +" --delimiter , --deposition WRITE_TRUNCATE --skiprows 1"
            os.system(loadtables)

    logging.info('All files Loaded  ....')
    
    '''Load Current Employee Dim Tables - cur_wd_employee'''
    logging.info('Loading CUR_WD_EMPLOYEE table ....')   
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[0]))
    
    '''Load Work Employee Dim Tables - work_wd_employee'''
    logging.info('Loading WORK_WD_EMPLOYEE table ....') 
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[3]).replace('V_FILE_ID',fileid)) 
    
    logging.info('Updating WORK_WD_EMPLOYEE table with Hash Values ....') 
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[5])) 
    
    '''Load Employee Dimension Tables - Employee Dim'''
    logging.info('Loading EMPLOYEE_DIM table ....') 
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[1]).replace('V_JOB_RUN_ID',str(jobrunid)).replace('V_JOB_NAME',str(jobname)))
    
    logging.info('Updating EMPLOYEE_DIM table ....')    
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[4])) 
    
    logging.info('Loading LOCATION_DIM table ....')    
    run_query(readfileasstring(cwd+"/sql/"+sqlfile[2]))
   
    ''' Move files to Archive folder '''
    logging.info("Archiving files...")
    movefiles= "gsutil mv gs://"+product_config['bucket']+"/current/*"+" gs://"+product_config['bucket']+"/archive/"
    print(movefiles)
    os.system(movefiles)
    logging.info("Employee Dimension Loading process completed..")
              
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


    
    
    



    

       