'''

Created on August 7 ,2019

@author: Ramneek Kaur

This module reads data from Sybase and Write into Bigquery Table

It uses pandas dataframe to read and write data to BigQuery.

'''



 

import apache_beam as beam

import time

import jaydebeapi 

import os

import argparse

from google.cloud import bigquery

import logging

import sys

import base64

from google.cloud import storage as gstorage

import pandas

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





class setenv(beam.DoFn): 

      def process(self,context):

          os.system('gsutil cp '+product_config['stagingbucket']+'/jdbc-4.10.8.1.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )

          logging.info('Jar copied to Instance..')

          logging.info('Java Libraries copied to Instance..')

          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')

          logging.info('Enviornment Variable set.')

          return list("1")

      

      

class readfrominformix(beam.DoFn): 

      def process(self, context, inquery,targettable):

          database_user=env_config[connectionprefix+'_database_user']

          database_password=env_config[connectionprefix+'_database_password']

          database_host=env_config[connectionprefix+'_database_host']

          database_database=env_config[connectionprefix+'_database']

          database_port=env_config[connectionprefix+'_database_port']

          

          jclassname = "com.informix.jdbc.IfxDriver"

          url = ('jdbc:informix-sqli://10.140.3.50:50000/cms:INFORMIXSERVER=cms_net;user=gcpdata;password=wintel1;')

          jars = ["/tmp/jdbc-4.10.8.1.jar"]

          libs = None

          cnx = jaydebeapi.connect(jclassname, url,[database_user,database_password], jars=jars,libs=libs) 

          print(cnx)

          logging.info('Reading Sql Query from the file...')

          query = inquery.replace('jobrunid',str(jobrunid)).replace('jobname', jobname).replace('v_inputdate',inputdate)

          logging.info('Query is %s',query)

          logging.info('Query submitted to informix Database')



          logging.info("Started loading Table")

          for chunk in pandas.read_sql(query.replace("v_database_name", database_database), cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=200000):

              chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='replace')

             

          logging.info("Load completed...")

          return list("1")   

      

      

class runbqsql(beam.DoFn):

    def process(self, context, inputquery):

          client = bigquery.Client(project=env_config['projectid'])

          query=inputquery

          query_job = client.query(query)

          results = query_job.result()

          print(results)

          return list("1") 

       

       

              



def run():

    """

    1. Set Dataflow PipeLine configurations.

    2. Create PCollection element for each line read from the delimited file.

    3. Tag values by calling RowValidator method i.e. clean records or broken records.

    4. Call rowextractor method for cleaned records.

    5. Write valid/clean records to BigQuery table mentioned in the parameter.

    6. Sink the error records to error handler.  

    """

    pipeline_args = ['--project', env_config['projectid'],

                     '--job_name', jobname,

                     '--runner', env_config['runner'],

                     '--staging_location', product_config['stagingbucket'],

                     '--temp_location', product_config['tempbucket'],

                     '--requirements_file', env_config['requirements_file'],

                     '--region', env_config['region'],

                     '--zone',env_config['zone'],

                     '--network',env_config['network'],

                     '--subnetwork',env_config['subnetwork'],

                     '--save_main_session', 'True',

                     '--num_workers', env_config['num_workers'],

                     '--max_num_workers', env_config['max_num_workers'],

                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],

                     '--service_account_name', env_config['service_account_name'],

                     '--service_account_key_file', env_config['service_account_key_file'],

                     '--worker_machine_type', "n1-standard-8",

                     '--extra_package', env_config['extra_package']

                    ]

   

    

    try:

        

        pcoll = beam.Pipeline(argv=pipeline_args)

        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])

        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())

        



    

        

    

        (dummy_env | 'hvector load ' >>  beam.ParDo(readfrominformix(),landing_av_hvector,'WORK_AV_HVECTOR')

         | 'hVECTOR' >>  beam.ParDo(runbqsql(),insert_hvector))

        (dummy_env | 'hvdn load ' >>  beam.ParDo(readfrominformix(),landing_av_hvdn,'WORK_AV_HVDN')

         | 'HVDN' >>  beam.ParDo(runbqsql(),insert_hvdn))

        (dummy_env | 'truck load ' >>  beam.ParDo(readfrominformix(),landing_av_htrunk,'WORK_AV_HTRUNK')

        | 'HTRUNK' >>  beam.ParDo(runbqsql(),insert_htrunk))

        (dummy_env | 'dvector load ' >>  beam.ParDo(readfrominformix(),landing_av_dvector,'WORK_AV_DVECTOR')

         | 'DVECTOR' >>  beam.ParDo(runbqsql(),insert_dvector))

        (dummy_env | 'dvdn load ' >>  beam.ParDo(readfrominformix(),landing_av_dvdn,'WORK_AV_DVDN')

        | 'DVDN' >>  beam.ParDo(runbqsql(),insert_dvdn))

        (dummy_env | 'dtruck load ' >>  beam.ParDo(readfrominformix(),landing_av_dtrunk,'WORK_AV_DTRUNK')

         | 'DTRUNK' >>  beam.ParDo(runbqsql(),insert_dtrunk))

        (dummy_env | 'agroups load ' >>  beam.ParDo(readfrominformix(),landing_av_agroups,'WORK_AV_AGROUPS')

         | 'AGROUPS' >>  beam.ParDo(runbqsql(),insert_av_agroups))

        (dummy_env | 'ag_actv load ' >>  beam.ParDo(readfrominformix(),landing_av_ag_actv,'WORK_AV_AG_ACTV')

         | 'ACTV' >>  beam.ParDo(runbqsql(),insert_ag_actv))

        

        

        

        

#         (dummy_env | 'wsplit load ' >>  beam.ParDo(readfrominformix(),landing_av_wsplit,'WORK_AV_WSPLIT')

#         | 'WSPLIT' >>  beam.ParDo(runbqsql(),insert_wsplit))

#         (dummy_env | 'wagnet load' >>  beam.ParDo(readfrominformix(),landing_av_wagnet,'WORK_AV_WAGENT')

#         | 'WAGENT' >>  beam.ParDo(runbqsql(),insert_wagent))

#         (dummy_env | 'msplit load ' >>  beam.ParDo(readfrominformix(),landing_av_msplit,'WORK_AV_MSPLIT')

#         | 'MSPLIT' >>  beam.ParDo(runbqsql(),insert_msplit))

#         (dummy_env | 'magnet load ' >>  beam.ParDo(readfrominformix(),landing_av_magent,'WORK_AV_MAGENT')

#         | 'MAGENT' >>  beam.ParDo(runbqsql(),insert_magent))

#         (dummy_env | 'hsplits load ' >>  beam.ParDo(readfrominformix(),landing_av_hsplits,'WORK_AV_HSPLIT')

#         | 'HSPLIT' >>  beam.ParDo(runbqsql(),insert_hsplit))

#         (dummy_env | 'haglog load ' >>  beam.ParDo(readfrominformix(),landing_av_haglog,'WORK_AV_HAGLOG')

#         | 'HAGLOG' >>  beam.ParDo(runbqsql(),insert_haglog))

#         #(dummy_env | 'hagent load ' >>  beam.ParDo(readfrominformix(),landing_av_hagent,'WORK_AV_HAGENT')

#         (dummy_env | 'dsplit load ' >>  beam.ParDo(readfrominformix(),landing_av_dsplit,'WORK_AV_DSPLIT')

#         | 'DSPLIT' >>  beam.ParDo(runbqsql(),insert_dsplit))

#         (dummy_env | 'dagent load ' >>  beam.ParDo(readfrominformix(),landing_av_dagent,'WORK_AV_DAGENT')

#         | 'DAGENT' >>  beam.ParDo(runbqsql(),insert_dagent))

#         

#         

#         

         



                 

                 

        p = pcoll.run()

        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise    



def main(args_config,args_productconfig,args_env,args_connectionprefix,args_incrementaldate,args_inputdate):

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

    global connectionprefix

    connectionprefix = args_connectionprefix

    global incrementaldate

    incrementaldate=args_incrementaldate

    global sqlstring

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)

    global jobname

    jobname="load"+product_config['productname']+"-"+"land"+"-"+time.strftime("%Y%m%d")

    logging.info('Job Name is %s',jobname)

    global inputdate

    inputdate = args_inputdate

    logging.info('Job Name is %s',jobname)

    

   

    global landing_av_wsplit

    global landing_av_wagnet

    global landing_av_msplit

    global landing_av_magent

    global landing_av_hvector

    global landing_av_hvdn

    global landing_av_htrunk

    global landing_av_hsplits

    global landing_av_haglog

    global landing_av_hagent

    global landing_av_dvector

    global landing_av_dvdn

    global landing_av_dtrunk

    global landing_av_dsplit

    global landing_av_dagent

    global landing_av_agroups

    global landing_av_ag_actv

    

    

   

    landing_av_wsplit = readfileasstring(cwd + '/sql/work_wsplit.sql')

    landing_av_msplit = readfileasstring(cwd + '/sql/work_mspilt.sql')

    landing_av_wagnet = readfileasstring(cwd + '/sql/work_wagnet.sql')

    landing_av_magent = readfileasstring(cwd + '/sql/work_magent.sql')

    landing_av_hvector = readfileasstring(cwd + '/sql/work_hvector.sql')

    landing_av_hvdn = readfileasstring(cwd + '/sql/work_hvdn.sql')

    landing_av_htrunk = readfileasstring(cwd + '/sql/work_htrunk.sql')

    landing_av_hsplits = readfileasstring(cwd + '/sql/work_hsplits.sql')

    landing_av_haglog = readfileasstring(cwd + '/sql/work_haglog.sql')

    landing_av_hagent = readfileasstring(cwd + '/sql/work_hagent.sql')

    landing_av_dvector = readfileasstring(cwd + '/sql/work_dvector.sql')

    landing_av_dvdn = readfileasstring(cwd + '/sql/work_dvdn.sql')

    landing_av_dtrunk = readfileasstring(cwd + '/sql/work_dtrunk.sql')

    landing_av_dsplit = readfileasstring(cwd + '/sql/work_dsplit.sql')

    landing_av_dagent = readfileasstring(cwd + '/sql/work_dagent.sql')

    landing_av_agroups = readfileasstring(cwd + '/sql/work_agroups.sql')

    landing_av_ag_actv = readfileasstring(cwd + '/sql/work_ag_actv.sql')

    

    

    

    

    global insert_ag_actv

    global insert_av_agroups

    global insert_dtrunk

    global insert_dvector

    global insert_hvector

    global insert_hvdn

    global insert_dvdn

    global insert_htrunk

    

    insert_ag_actv = readfileasstring(cwd +'/sql/ag_actv.sql')

    insert_av_agroups = readfileasstring(cwd +'/sql/agroups.sql')

    insert_dtrunk = readfileasstring(cwd +'/sql/dtrunk.sql')

    insert_dvector = readfileasstring(cwd +'/sql/dvector.sql')

    insert_hvector = readfileasstring(cwd +'/sql/hvector.sql')

    insert_hvdn = readfileasstring(cwd +'/sql/hvdn.sql')

    insert_dvdn = readfileasstring(cwd +'/sql/dvdn.sql')

    insert_htrunk = readfileasstring(cwd +'/sql/htrunk.sql')

    

    

    global insert_dsplit

    global insert_hsplit

    global insert_hagent

    global insert_dagent

    global insert_wsplit

    global insert_msplit

    global insert_wagent

    global insert_magent

    global insert_haglog

    

    

    insert_dsplit = readfileasstring(cwd +'/sql/dsplit.sql')

    insert_hsplit = readfileasstring(cwd +'/sql/hsplit.sql')

    insert_hagent = readfileasstring(cwd +'/sql/hagent.sql')

    insert_dagent = readfileasstring(cwd +'/sql/dagent.sql')

    insert_wsplit = readfileasstring(cwd +'/sql/wsplit.sql')

    insert_msplit = readfileasstring(cwd +'/sql/msplit.sql')

    insert_wagent = readfileasstring(cwd +'/sql/wagent.sql')

    insert_magent = readfileasstring(cwd +'/sql/magent.sql')

    insert_haglog = readfileasstring(cwd +'/sql/haglog.sql')

    

    

    

    

    

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

        '--connectionprefix',

        required=True,

        help= ('connectionprefix either Schema')

        )

    

    parser.add_argument(

        '--incrementaldate',

        required=False,

        help= ('Incremental Data Pull Filter date')

        )

    parser.add_argument(

        '--inputdate',

        required=False,

        help= ('Incremental Data Pull Filter date')

        )



    args = parser.parse_args()   

        

    main(args.config,

         args.productconfig,

         args.env,

         args.connectionprefix,

         args.incrementaldate,

         args.inputdate)    