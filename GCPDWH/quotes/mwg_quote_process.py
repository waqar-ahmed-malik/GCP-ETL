'''

Created on May 22, 2019

@author: Aarzoo Malik

'''



from datetime import datetime, timedelta

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging

from google.api.logging_pb2 import Logging



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"



else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"



def readfileasstring(sqlfile):     

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

    global fileids

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)

    current_date=(datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    print(current_date)

    global jobname

    jobname="load_MWG"+product_config['productname']+"_"+ current_date

    logging.info('Job Name is %s',jobname)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]

    os.listdir(cwd+'/sql/')

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    

    logging.info('Started Loading Files  ....')

       

    for blob in bucket.list_blobs(prefix='current/'):

        if blob.name.endswith(current_date+".csv"):

            filename=blob.name

            targettable= 'WORK_MWG_QUOTE'

            fileid=filename.split('_')

            fileids = fileid[2].split('.')[0]

            print(fileids)

            print("gs://"+product_config['bucket']+"/"+filename)

            logging.info('Loading %s file into %s table ',filename,targettable)

            loadtables_az="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig mwgquote.properties --env "+env +" --targettable " + targettable + " --filename "+'"'+ filename +'"'+" --delimiter , --deposition WRITE_TRUNCATE --skiprows 1"

            os.system(loadtables_az)

               

            logging.info('load MWG_STG_INSURANCE_QUOTE table ....')    

            run_query(readfileasstring(cwd+"/sql/merge_mwg_stg_insurance_quote.sql"))

            logging.info('load MWG_STG_INSURANCE_QUOTE table ....')

   

            logging.info('Merge work_quotes_dim table ....')    

            run_query(readfileasstring(cwd+"/sql/work_quotes_dim_insert_mwgquotes.sql").replace('V_FILE_ID',str(fileids).replace('v_job_run_id',str(jobrunid))))

            logging.info('Merged work_quotes_dim table ....')

             

            

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

    

    args = parser.parse_args()       

 

main(args.config,

     args.productconfig,

     args.env)
