'''

Created on Jan 15, 2019

@author: Prerna Anand

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

         #print(format(row))

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

    jobname="load_"+product_config['productname']+"_"+"quotes_dim"

    logging.info('Job Name is %s',jobname)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]

    os.listdir(cwd+'/sql/')  

    #print product_config['bucket']

#     current_date= (datetime.today() - timedelta(days=1)).strftime('%Y_%b_%d').lstrip("0").replace("_0", "_")

    current_date= datetime.today().strftime('%Y_%b_%d').lstrip("0").replace("_0", "_")



    print(current_date)

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    

    logging.info('Truncating WORK_PAS_QUOTES table ....')    

    run_query(readfileasstring(cwd+"/sql/truncate_pasquotes.sql"))

    

    logging.info('Started Loading Files  ....')

       

    for blob in bucket.list_blobs(prefix='current/Quote+Summary+Report'):



        filename=blob.name#.split('/')[1]

        if current_date in filename:

            print(filename)

            targettable='WORK_PAS_QUOTES'

            fileid=filename.split('.')[0].split('_')

            

            fileids = [ fileid[1],fileid[2],fileid[3] ]

            fileid=''.join(fileids)

            print(fileid)

            

            logging.info('Loading %s file into %s table ',filename.split('/')[1],targettable)

            loadtables_az="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig quotes.properties --env "+env +" --targettable " + targettable + " --filename "+'"'+ filename +'"'+" --delimiter , --deposition WRITE_APPEND --skiprows 1"

            os.system(loadtables_az)

            logging.info('file loaded')

                    

             

            logging.info('Truncating WORK_QUOTES_DIM table ....')    

            run_query(readfileasstring(cwd+"/sql/truncate_work_quotes_dim.sql"))

            logging.info('Truncated WORK_QUOTES_DIM table ....') 

             

            logging.info('Loading WORK_QUOTES_DIM table ....')    

            run_query(readfileasstring(cwd+"/sql/work_quotes_dim_insert.sql").replace('V_FILE_ID',fileid).replace('v_job_run_id',str(jobrunid)))

            logging.info('Loaded WORK_QUOTES_DIM table ....') 

             

            logging.info('Loading QUOTES_DIM table ....')    

            run_query(readfileasstring(cwd+"/sql/insert_quotes_dim_type2.sql"))

            logging.info('Loaded QUOTES_DIM table ....') 

             

            logging.info('Updating QUOTES_DIM table ....')    

            run_query(readfileasstring(cwd+"/sql/update_date_in_quotes.sql"))

            logging.info('Updated QUOTES_DIM table ....') 



    logging.info('All files Loaded  ....')



    

    '''

    

    logging.info("Archiving files...")

    movefiles= "gsutil mv gs://"+product_config['bucket']+"/current/*"+" gs://"+product_config['bucket']+"/archive/"

    print movefiles

    os.system(movefiles)

    logging.info("Employee Dimension Loading process completed..")

    '''          

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





       
