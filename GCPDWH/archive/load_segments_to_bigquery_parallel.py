'''
Created on March 30, 2018
This module loads a Bigquery Table from Google Cloud Storage bucket.
Module needs bucket name to be read and destination Bigquery Table name.
It picks up the environment and product information from respective config files
job name for pipeline is generated based on dataset name and table name
@author: Rajnikant Rakesh
'''

import apache_beam as beam
from apache_beam import pvalue
from apache_beam import pvalue
import argparse
import logging
import os
import sys
from google.cloud import bigquery
from string import lower
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
from gettext import find

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
      
def read_files(pipeline):
    collections = []
    pcollectionlist =[]
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket(product_config['bucket'])
    #logging.info('Started Loading Files  ....')
    blob=bucket.list_blobs(prefix='ivans/current/')
    print(blob)
    
    for blob in bucket.list_blobs(prefix='ivans/current/IE'):
        try:
            filename=blob.name#.split('/')[1]
            print(filename)
            file="gs://"+product_config['bucket']+"/"+filename
            print(file)
            global segment
            global filedate
            segment=filename.split('/')[2].split('_')[1]
            filedate=filename.split('/')[2].split('_')[3].split('.')[0]
            collection = pipeline | ('Read  Segment  %s' % segment) >> beam.io.ReadFromText(file,skip_header_lines = remove_header_rows)
            collections.append(collection)
        except IOError:
            logging.error("Failed to read ")    
    
    return collections

def main(args_config,args_productconfig,args_env,args_srcsystem):
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
    global srcsystem
    srcsystem=args_srcsystem
    global jobdate
    jobdate=time.strftime("%m%d%Y")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket(product_config['bucket'])
    logging.info('Started Loading Files  ....')
    for blob in bucket.list_blobs(prefix='ivans/current/IE'):
        try:
            filename=blob.name#.split('/')[1]
            print(filename)
            file="gs://"+product_config['bucket']+"/"+filename
            print(file)
            global segment
            global filedate
            global tablename
            segment=filename.split('/')[2].split('_')[1]
            filedate=filename.split('/')[2].split('_')[3].split('.')[0]
            tablename=srcsystem+"_"+segment+"_LDG_1"
            calldataflow="python "+utilpath+"load_csv_to_bigquery_dataflow_withpolling.py "+" --config config.properties --productconfig ivans.properties --env dev" + " --separator "+'"|"'+"  --stripheader 0 --stripdelim 0  --addaudit 0  --writeDeposition WRITE_TRUNCATE --input "+file+" --output "+ tablename+" --dfjobtag "+jobdate
            
            print(calldataflow)
            os.system(calldataflow)
            #collection = pipeline | ('Read  Segment  %s' % segment) >> beam.io.ReadFromText(file,skip_header_lines = remove_header_rows)
            #collections.append(collection)
        except IOError:
            logging.error("Failed to read ")
        

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
        '--srcsystem',
        required=True,
        help= ('AS400/IE')
        )
    
    args = parser.parse_args()       
 
main(args.config,
     args.productconfig,
     args.env,
     args.srcsystem)       
    