'''

Created on Aug 22, 2019

@author: Atul Guleria

'''

from datetime import datetime, timedelta

from datetime import datetime

import os

import argparse


from google.cloud import bigquery

from google.cloud import storage

import logging



now = datetime.now()



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

print(utilpath)





def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):

    """Copies a blob from one bucket to another with a new name."""

    storage_client = storage.Client()

    source_bucket = storage_client.get_bucket(bucket_name)

    source_blob = source_bucket.blob(blob_name)

    destination_bucket = storage_client.get_bucket(new_bucket_name)



    new_blob = source_bucket.copy_blob(

        source_blob, destination_bucket, new_blob_name)



    print(('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(

        source_blob.name, source_bucket.name, new_blob.name,

        destination_bucket.name)))

    

    

def delete_blob(bucket_name, blob_name):

    """Deletes a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)



    blob.delete()



    print(('Blob {} deleted.'.format(blob_name)))



def readfileasstring(sqlfile): 

      

     """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 

     with open (sqlfile, "r") as file:

         sqltext=file.read().strip("\n").strip("\r")

     return sqltext

 

def run_query(file_name):

     client = bigquery.Client(project=env_config['projectid'])

     query_job = client.query(readfileasstring(os.path.abspath(cwd+'/sql/insert_comm_log.sql')).replace('file_nm',file_name).replace("v_job_run_id",str(jobrunid)).replace("v_job_name",("Load-" + product_config['datasetname'] )))

     results = query_job.result()

     print(results)

         

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

    current_date=datetime.today() - timedelta(days=1)

    print(current_date)

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    for blob in bucket.list_blobs(prefix='comm-logb/current/'):

        if blob.name.split('/')[2]:

            filename=blob.name.split('/')[0]+'/'+blob.name.split('/')[1]+'/'+blob.name.split('/')[2]

            print(filename)

            archive_filename='comm-logb/archive/'+blob.name.split('/')[2]

            print(bucket)

            stg_table_name='WORK_COMM_LOG'

            main_table_name='COMM_LOG'

            sql_file='insert_comm_log.sql'

            skip_lead_rows='1'

            print("WORK_COMM_LOG") 

                                          

            loadstgtables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig commlog.properties --env " +env+ " --targettable " + stg_table_name + " --filename "+ filename +" --delimiter \"|\" --deposition WRITE_TRUNCATE --skiprows "+skip_lead_rows

            os.system(loadstgtables)

            logging.info('Loading main table...')

            run_query(blob.name.split('/')[2])

            logging.info('Main table loaded...')

            copy_blob(product_config['bucket'], filename, product_config['bucket'], archive_filename)

            logging.info('Files copied to Archive...')

            delete_blob(product_config['bucket'], filename)

            logging.info('Files deleted from current...')

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
