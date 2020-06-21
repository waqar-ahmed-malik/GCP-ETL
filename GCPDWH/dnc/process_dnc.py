'''
Created on Aug 17, 2018
@author: Prerna Anand
'''
from datetime import datetime
import os
import argparse
from string import  upper
from google.cloud import bigquery
from google.cloud import storage
from string import upper
import logging

now = datetime.now()

cwd = os.path.dirname(os.path.abspath(__file__))
if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
   
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"


def main(args_config,args_productconfig,args_env,args_inputdate):
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
    jobname="load_"+product_config['productname']
    global inputdate
    inputdate=args_inputdate
    logging.info('Job Name is %s',jobname)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket(product_config['bucket'])
    print(bucket)
    logging.info('Started Loading Files  ....')
    for blob in bucket.list_blobs(prefix='current/'):
        print(blob)
        filename=blob.name.split('/')[0]+'/'+blob.name.split('/')[1]
        print(filename)
        flag=1
        if  'DNC' in filename and inputdate in filename:
            stg_table_name='WORK_CSS_DO_NOT_CONTACT'
            main_table_name='CSS_DO_NOT_CONTACT'
            sql_file='insert_css_do_not_contact.sql'
            skip_lead_rows='1'
            print("1 WORK CSS DO NOT CONTACT")  
                                                                                  
        else:
            flag=0        
        if flag == 1:        
         print(stg_table_name)
         print(main_table_name)        
         loadstgtables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig dnc.properties --env " + env + " --targettable " + stg_table_name + " --filename "+ filename +" --delimiter ^| --deposition WRITE_TRUNCATE --skiprows "+skip_lead_rows
         print(loadstgtables)
         os.system(loadstgtables)
         loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig dnc.properties --env "+ env + " --sqlfile "+ sql_file + " --tablename " + main_table_name         
         os.system(loadmaintables)         
         logging.info('All files Loaded  ....')
    

              
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
        '--inputdate',
        required=True,
        help= ('execution date from airflow')
        )
            
    args = parser.parse_args()       
 
main(args.config,
     args.productconfig,
     args.env,
     args.inputdate)       
