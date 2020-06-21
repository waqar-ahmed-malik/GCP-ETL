'''

Created on Sep 18, 2019

@author: Maniratnam Patchigolla

'''

from datetime import datetime,timedelta

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

from string import upper

import logging



now = datetime.now()- timedelta(days=1)

print(now)



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    homepath = cwd[:folderup]+"\\discover\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    homepath = cwd[:folderup]+"/discover/"



def readfileasstring(sqlfile):     

     """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 

     with open (sqlfile, "r") as file:

         sqltext=file.read().strip("\n").strip("\r")

     return sqltext



def delete_blob(bucket_name, blob_name):

    """Deletes a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)

    blob.delete()

    print(('Blob {} deleted.'.format(blob_name)))



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

    global fileid

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)

    global jobname

    jobname="load_"+product_config['productname']

    logging.info('Job Name is %s',jobname)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    for blob in bucket.list_blobs(prefix='discover-payment-transactions/current/'):

        

        gs = "gs://"

        filename=gs+product_config['bucket']+'/'+blob.name.split('/')[0]+'/'+blob.name.split('/')[1]+'/'+blob.name.split('/')[2]

        print(filename)

        file = blob.name.split('/')[0]+'/'+blob.name.split('/')[1]+'/'+blob.name.split('/')[2]

        print(file)

        

        flag=1

        if 'AAAMWGO.MERSDLY' in filename :

            main_table_name='DISCOVER_PAYMENTS'      

            main_table_name_1='DISCOVER_PAYMENT_FAILURES'      

            sql_file='insert_discover_payments.sql'

            sql_file_1='insert_discover_payment_failures.sql'

            loaddiscovertables="python "+homepath+"load_discover_to_bigquery_landing.py "+ "--config config.properties --productconfig discover.properties --env "+env+" --input " + filename +" --separator ^| --stripheader 0 --stripdelim 0 --addaudit 0 --writeDeposition WRITE_TRUNCATE"

            os.system(loaddiscovertables)

            loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig discover.properties --env "+env+" --sqlfile "+ sql_file + " --tablename " + main_table_name

            os.system(loadmaintables) 

            loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig discover.properties --env "+env+" --sqlfile "+ sql_file_1 + " --tablename " + main_table_name_1    

            os.system(loadmaintables)

 

            delete_blob(product_config['bucket'], file)

            logging.info('Files deleted from current...')

         

        else:

            flag=0 

 

    logging.info('Files Loaded ....')

    



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
