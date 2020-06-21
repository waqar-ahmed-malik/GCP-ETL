'''

Created on August 01, 2018

This dataflow pipeline transfers files from SFTP location to GCS bcukets.

This pipelines needs to be executed on a VPC/Shared network which is connected to a Global VPN to in-prem network. 

This pipeline supports both single and bulk files transfer from FTP location.



SFTP configurations, mount point location and  bucket details are read from respective product configurations file.

@author: Rajnikant Rakesh



Arguments Required : 

Eg :

--config "config.properties" --productconfig "pcomp.properties" --env "dev"  

'''





import apache_beam as beam

import argparse

import logging

import os

import sys

import pysftp

import base64

from googleapiclient import discovery

from oauth2client.client import GoogleCredentials

import time

from oauth2client.service_account import ServiceAccountCredentials

from google.cloud import storage






cwd = os.path.dirname(os.path.abspath(__file__))

#bucket=['dw-prod-msi-results', 'dw-prod-napatracs', 'dw-prod-mac-triptik', 'dw-prod-vendnovation-kiosk','dw-prod-msi','dw-prod-msi-skip'];



if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    



class connecttosftp(beam.DoFn):

    def process(self,

                context

                ):

        files= product_config['mountpointpath']

        filelist= files.split()

        cnopts = pysftp.CnOpts()

        cnopts.hostkeys = None

        logging.info("Connecting to Mount Point server..")
        
        database_password=env_config['ftppassword']
        
        with pysftp.Connection(host=env_config['ftpserver'],port=22, username=env_config['ftpuser'],password=str(base64.b64decode(database_password.encode('utf-8')), 'utf-8'), cnopts = cnopts) as sftp:

            logging.info("Connected to Mount Point server..")

            for loc in filelist:

                logging.info("loc is "+loc)

                loc_split=loc.split('/')

                bucket_name=loc_split[3].split('_')[1]+'-'+loc_split[4]+'-msi'

                logging.info("bucket_name is "+bucket_name)

                sftp.cwd(loc)

                for f in sftp.listdir_attr():

                    sftp.get(f.filename,'/tmp/'+f.filename)            

                    if f.filename.endswith(".gz"):

                        os.system('gzip -d /tmp/' + f.filename)

                        unzip_file = f.filename.rsplit( ".", 1 )[ 0 ]

                        os.system('iconv -f ASCII -t UTF-8//IGNORE /tmp/'+unzip_file +' -o /tmp/'+unzip_file+'_temp')

                        os.system('mv /tmp/'+unzip_file+'_temp /tmp/'+unzip_file)

                        #sftp.remove(f.filename)  

                    else:                

                        os.system('iconv -f ASCII -t UTF-8//IGNORE /tmp/'+f.filename +' -o /tmp/'+f.filename+'_temp')

                        os.system('mv /tmp/'+f.filename+'_temp /tmp/'+f.filename)

                        #sftp.remove(f.filename)

                        

                os.system('gsutil -m mv /tmp/* '+'gs://dw-prod-'+bucket_name+'/current/')



def run():

    """

    1. Set Dataflow PipeLine configurations.

    2. Call the method to transfer files from SFTP.  

    """

    

    pipeline_args = ['--project', env_config['projectid'],

                     '--job_name', jobname,

                     '--runner', env_config['runner'],

                     '--staging_location', product_config['stagingbucket'],

                     '--temp_location', product_config['tempbucket'],

                     '--requirements_file', env_config['requirements_file'],

                     '--save_main_session', 'True',

                     '--region', env_config['region'],

                     '--zone',env_config['zone'],

                     '--network',env_config['network'],

                     '--subnetwork',env_config['subnetwork'],

                     '--num_workers', env_config['num_workers'],

                     '--max_num_workers', env_config['max_num_workers'],

                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],

                     '--service_account_name', env_config['service_account_name'],

                     '--service_account_key_file', env_config['service_account_key_file']

                     ]

    

    try:



        pcoll = beam.Pipeline(argv=pipeline_args)

        connectmountpoint= pcoll | 'Connecting Mount Point' >> beam.Create(['1'])

        connectmountpoint | 'Reading Files' >> beam.ParDo(connecttosftp())

        p=pcoll.run()

        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise    





def main(args_config,args_productconfig,args_env):

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

    global jobname

    batchdate = time.strftime("%Y%m%d")

    jobname="copy-file-from-"+product_config['productname']+"-"+batchdate

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

    



    args = parser.parse_args()   

        

    main(args.config,

         args.productconfig,

         args.env)
