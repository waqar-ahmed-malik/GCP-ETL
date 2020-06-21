'''

Created on Aug 16, 2018

@author: Rajnikant Rakesh



'''

import datetime

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"



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

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    for blob in bucket.list_blobs(prefix='Quote+Summary+User+Agency+Report_2016_Jan1-Dec31.csv'):

        filename=blob.name#.split('/')[1]

        print(filename)

        #file= print '"{}"'.format(filename) 

        #file="gs://"+product_config['bucket']+"/"+filename

        loadtables="C:\\Python27\\python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig quotes.properties --env prod --targettable WORK_PAS_QUOTES_1  --filename "+ '"'+filename+'"' +" --delimiter , --deposition WRITE_APPEND --skiprows 1"

        print(loadtables)

        os.system(loadtables)



    logging.info("Quotes Loading process completed..")

              

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
