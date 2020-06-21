'''

Created on Sep 27, 2018

@author: Ramneek Kaur

This module reads data from Sqlserver and Write into Bigquery Table

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

    mdmpath = cwd[:folderup]+"\\mdm\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    mdmpath = cwd[:folderup]+"/mdm/"



def readfileasstring(sqlfile):   

    """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 

    with open (sqlfile, "r") as file:

        sqltext=file.read().strip("\n").strip("\r")

    return sqltext





class setenv(beam.DoFn): 

      def process(self,context):

          os.system('gsutil cp '+product_config['stagingbucket']+'/sqljdbc41.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )

          logging.info('Jar copied to Instance..')

          logging.info('Java Libraries copied to Instance..')

          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')

          logging.info('Enviornment Variable set.')

          return list("1")

      

      

class readfromssql(beam.DoFn): 

      def process(self, context, inquery,targettable):

#           element=sqlstring[0]

          database_user=env_config[connectionprefix+'_database_user']

#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')
          database_password = env_config[connectionprefix + '_database_password']
          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')
          database_host=env_config[connectionprefix+'_database_host']

          database_database=env_config[connectionprefix+'_database']

          database_port=env_config[connectionprefix+'_database_port']

          

          jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

          url = ("jdbc:sqlserver://" + database_host + ':' + database_port + ';database=' + database_database + ';user=' + database_user + ';password=' + database_password)

          jars = ["/tmp/sqljdbc41.jar"]

          libs = None

          cnx = jaydebeapi.connect(jclassname, url, jars=jars,

                            libs=libs)   

          logging.info('Connection Successful..')

          cnx.cursor()

          logging.info('Reading Sql Query from the file...')

          query = inquery.replace('v_incr_date',incrementaldate).replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

          logging.info('Query is %s',query)

          logging.info('Query submitted to SqlServer Database')



          logging.info("Started loading Table")

          for chunk in pandas.read_sql(query.replace("v_database_name", database_database), cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=500000):

              chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='append')

             

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

      

def runbqsql1(dummy,inputquery):

          client = bigquery.Client(project=env_config['projectid'])

          query=inputquery

          query_job = client.query(query)

          results = query_job.result()

          print(results)

          for row in results:

              global maxdlykey

              maxdlykey = str(row[0])

          return maxdlykey        

      

class runbqsqloop(beam.DoFn):

    def process(self, context, inputquery):

          client = bigquery.Client(project=env_config['projectid'])

          inputquerysplit=inputquery.split(';')

          for inputsql in inputquerysplit:

            query=inputsql

            query_job = client.query(query)

            if query_job.state == 'RUNNING':

                time.sleep(2)

            elif query_job.state == 'FAILURE':

                sys.exit("QUERY failed")            

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

                     '--worker_machine_type', "n1-standard-8"

                    #  '--extra_package', env_config['extra_package']

                    ]

   

    

    try:

        

        pcoll = beam.Pipeline(argv=pipeline_args)

        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])

        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())

        

        #LAND ALL TABLES FIRST

#         get_max_daily_key=(dummy_env | 'GET DAILY KEY ' >>  beam.Map(runbqsql1,get_max_key))
# 
        cs_member_transaction= (dummy_env | 'WORK CS MEMBER TRANSACTION ' >> beam.ParDo(readfromssql(),landing_cs_member_transaction, 'WORK_CS_MEMBER_TRANSACTIONS_FACT_jan16')) 

        cs_membership_customer= (cs_member_transaction | 'WORK CS MEMBERSHIP CUSTOMER' >> beam.ParDo(readfromssql(),landing_cs_member_customer,'WORK_CS_MEMBERSHIP_CUSTOMER_DIM_jan16'))    

#         cs_member = (cs_member_transaction | 'WORK CS MEMBER' >>  beam.ParDo(readfromssql(),landing_cs_member,'WORK_CS_MEMBER_DIM'))

        cs_membership= (cs_membership_customer| 'WORK CS MEMBERSHIP' >> beam.ParDo(readfromssql(),landing_cs_membership,'WORK_CS_MEMBERSHIP_DIM_jan16'))

#         cs_membership_customer= (cs_member_transaction | 'WORK CS MEMBERSHIP CUSTOMER' >> beam.ParDo(readfromssql(),landing_cs_member_customer,'WORK_CS_MEMBERSHIP_CUSTOMER_DIM'))    

		

		
# 
#         (cs_member_transaction | 'WORK auth_recycle_run_hist ' >> beam.ParDo(readfromssql(),landing_auth_recycle_run_hist, 'AUTH_RECYCLE_RUN_HIST')
# 
#         | 'CS cs_auth_recycle_run_hist' >>  beam.ParDo(runbqsql(),operational_cs_auth_recycle_run_hist))        
# 
#         
# 
#         cs_membership_md5_delete = (cs_membership | 'DELETE CS_MEMBERSHIP_MD5' >>  beam.ParDo(runbqsql(),delete_cs_membership_md5))
# 
#         cs_membership_md5=(cs_membership_md5_delete | 'WORK MD5 CS MEMBERSHIP' >>  beam.ParDo(runbqsql(),insert_membership_md5))
# 
#         
# 
#     
# 
#         #MD5 Tables now 
# 
#         
# 
#         cs_membership_work_mdm = (cs_membership_customer | 'WORK MDM MEMBERSHIP' >>  beam.ParDo(runbqsqloop(),load_mdm_stg_sql))
# 
#         cs_membership_processing_mdm = (cs_membership_work_mdm | 'MDM MEMBERSHIP' >>  beam.ParDo(runbqsqloop(),load_mdm_processing_sql))
# 
#         
# 
#         
# 
#         #MD5 tables 
# 
#         
# 
#         
# 
#         cs_member_md5_delete = (cs_membership_processing_mdm | 'DELETE CS_MEMBER_MD5' >>  beam.ParDo(runbqsql(),delete_cs_member_md5))
# 
#         cs_member_md5 = (cs_member_md5_delete | 'WORK MD5 CS_MEMBER' >>  beam.ParDo(runbqsql(),insert_member_md5))
# 
#         
# 
#         
# 
#         cs_membership_customer_md5_delete = (cs_membership_processing_mdm | 'DELETE CS_MEMBERSHIP_CUSTOMER_MD5' >>  beam.ParDo(runbqsql(),delete_cs_member_customer_md5))
# 
#         cs_membership_customer_md5=(cs_membership_customer_md5_delete  | 'WORK MD5 CS_MEMBERSHIP CUSTOMER' >>  beam.ParDo(runbqsql(),insert_member_customer_md5))
# 
#         
# 
#         
# 
#         # LOAD MART TABLES 
# 
#         
# 
#       
# 
#         cs_member_dim_remove_md5=(cs_member_md5 | 'CS MEMBER Dim remove MD5'>>  beam.ParDo(runbqsql(),remove_cs_member_md5))        
# 
#         cs_member_dim = (cs_member_dim_remove_md5| 'CS MEMBER Dim' >>  beam.ParDo(runbqsql(),merge_cs_member))
# 
#         cs_member_dim_type2=(cs_member_dim | 'CS MEMBER Dim Type2' >>  beam.ParDo(runbqsql(),type2_cs_member))
# 
#         
# 
#         
# 
#              
# 
#         cs_membership_dim_remove_md5=(cs_membership_md5 | 'CS MEMBERSHIP Dim remove MDd5' >>  beam.ParDo(runbqsql(),remove_cs_membership_md5))
# 
#         cs_membership_dim=(cs_membership_dim_remove_md5 | 'CS MEMBERSHIP Dim' >>  beam.ParDo(runbqsql(),merge_cs_membership))
# 
#         cs_membership_dim_type2=(cs_membership_dim | 'CS MEMBERSHIP Dim Type2' >>  beam.ParDo(runbqsql(),type2_cs_membership)) 
# 
#                   
# 
#         (cs_membership_customer_md5 | 'CS MEMBERSHIP CUSTOMER Dim remove MDd5' >>  beam.ParDo(runbqsql(),remove_cs_membership_customer_md5)
# 
#         | 'CS MEMBERSHIP CUSTOMER Dim' >>  beam.ParDo(runbqsql(),merge_cs_member_customer)
# 
#         | 'CS MEMBERSHIP CUSTOMER Dim Type2' >>  beam.ParDo(runbqsql(),type2_cs_member_customer))
# 
#    
# 
#          
# 
#         cs_member_transaction = (cs_member_dim_type2 | 'CS MEMBER TRANSACTION FACT RENEW UPDATE' >>  beam.ParDo(runbqsql(),update_transaction_fact)
# 
#         | 'CS MEMBER TRANSACTION Fact Maq ' >>  beam.ParDo(runbqsql(),merge_cs_member_transaction_maq))
# 
#         cs_member_transaction_fact = (cs_member_transaction| 'CS MEMBER TRANSACTION FACT' >>  beam.ParDo(runbqsql(),merge_cs_member_transaction))
# 
#        
# 
#          
# 
#          
# 
#          
# 
#          # load operational tables 
# 
#          
# 
#         (cs_member_dim_type2 | 'operational  MEMBER Dim..' >>  beam.ParDo(runbqsql(),operational_cs_member))
# 
#         (cs_membership_dim_type2 | 'operational MEMBERSHIP Dim..' >>  beam.ParDo(runbqsql(),operational_cs_membership))
# 
#         
# 
#         # AR enrollment tables 
# 
#          
# 
#          
# 
#         (cs_member_dim_type2 | 'land ar enrollment' >>  beam.ParDo(readfromssql(),landing_ar_enrollment,'WORK_MEMBERSHIP_AR_ENROLLMENT')
# 
#         | 'ar enrollment insert ' >>  beam.ParDo(runbqsql(),merge_ar_enrollment))  
# 
#         
# 
#         
# 
#         (cs_member_transaction_fact | 'WORK CS MBR' >> beam.ParDo(readfromssql(),landing_cs_mbr, 'WORK_MBR')
# 
# 		| 'CS MBR TABLE' >>  beam.ParDo(runbqsql(),merge_mbr))
# 
# 		
# 
#         (cs_member_transaction_fact | 'WORK CS MBRSHIP' >> beam.ParDo(readfromssql(),landing_cs_mbrship, 'WORK_MBRSHIP')
# 
# 		|'CS MBRSHIP TABLE' >>  beam.ParDo(runbqsql(),merge_mbrship))
# 
# 		
# 
#         (cs_member_transaction_fact | 'WORK CS RIDER' >> beam.ParDo(readfromssql(),landing_cs_rider, 'WORK_RIDER')
# 
# 		| 'CS RIDER TABLE' >>   beam.ParDo(runbqsql(),merge_rider))
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

    global inputdate

    inputdate = args_inputdate

    global sqlstring

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)

    global jobname

    jobname="loadlocalpy3-"+product_config['productname']+"-"+"land-member-dim"+"-"+time.strftime("%Y%m%d")

    logging.info('Job Name is %s',jobname)

    

    global landing_cs_mbr

    global landing_cs_mbrship

    global landing_cs_rider

    global merge_mbr

    global merge_mbrship

    global merge_rider 

	

    landing_cs_mbr = readfileasstring(cwd + '/sql/membership/work_mbr.sql')

    landing_cs_mbrship = readfileasstring(cwd + '/sql/membership/work_mbrship.sql')

    landing_cs_rider = readfileasstring(cwd + '/sql/membership/work_rider.sql')

	

    merge_mbr = readfileasstring(cwd + '/sql/membership/merge_mbr.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    merge_mbrship = readfileasstring(cwd + '/sql/membership/merge_mbrship.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    merge_rider = readfileasstring(cwd + '/sql/membership/merge_rider.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)



	

	

    global landing_cs_member

    global landing_cs_membership

    global landing_cs_member_customer

    global landing_cs_member_transaction

    global landing_ar_enrollment

    global landing_auth_recycle_run_hist

    global get_max_key

    

    landing_cs_member = readfileasstring(cwd + '/sql/membership/work_cs_member_dim.sql')

    landing_cs_membership = readfileasstring(cwd + '/sql/membership/work_cs_membership_dim.sql')

    landing_cs_member_customer = readfileasstring(cwd+'/sql/membership/work_cs_membership_customer_dim.sql')

    landing_cs_member_transaction = readfileasstring(cwd+ '/sql/membership/work_cs_member_transactions_fact.sql')

    landing_ar_enrollment =  readfileasstring(cwd+ '/sql/membership/work_membership_ar_enrollment.sql')

    landing_auth_recycle_run_hist =  readfileasstring(cwd+ '/sql/membership/work_mem_auth_recycle_run_hist.sql')

    get_max_key = readfileasstring(cwd+ '/sql/membership/get_max_dly_ky.sql')

    

    global insert_member_md5

    global insert_membership_md5

    global insert_member_customer_md5

    global merge_cs_member

    global merge_ar_enrollment

    global merge_cs_membership

    global merge_cs_member_customer

    global merge_cs_member_transaction

    global merge_cs_member_transaction_maq

    global operational_cs_member

    global operational_cs_membership

    global operational_cs_auth_recycle_run_hist

    global update_transaction_fact

    

    insert_member_md5 = readfileasstring(cwd + '/sql/membership/Insert_work_cs_member_dim_md5.sql')

    insert_membership_md5 = readfileasstring(cwd + '/sql/membership/Insert_work_cs_membership_dim_md5.sql')

    insert_member_customer_md5 = readfileasstring(cwd + '/sql/membership/Insert_work_cs_membership_customer_dim_md5.sql')

    

    

    merge_cs_member = readfileasstring(cwd + '/sql/membership/connectsuite_member_dim.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    merge_cs_membership = readfileasstring(cwd + '/sql/membership/connectsuite_membership_dim.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    merge_cs_member_customer = readfileasstring(cwd+'/sql/membership/membership_customer_dim.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    merge_cs_member_transaction = readfileasstring(cwd+ '/sql/membership/connectsuite_member_transactions_fact.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    merge_cs_member_transaction_maq = readfileasstring(cwd+ '/sql/membership/connectsuite_member_transaction_fact_maq.sql')

    merge_ar_enrollment = readfileasstring(cwd+ '/sql/membership/connectsuite_membership_ar_transaction_fact.sql')

    update_transaction_fact = readfileasstring(cwd+ '/sql/membership/connectsuite_members_transaction_fact_renew_update.sql')

    

    

    operational_cs_member = readfileasstring(cwd + '/sql/membership/operational_member_dim.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    operational_cs_membership = readfileasstring(cwd + '/sql/membership/operational_membership_dim.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    operational_cs_auth_recycle_run_hist = readfileasstring(cwd + '/sql/membership/insert_mem_auth_recycle_run_hist.sql')

    

    

    

    global type2_cs_member

    global type2_cs_membership

    global type2_cs_member_customer

    

    type2_cs_member = readfileasstring(cwd + '/sql/membership/update_connectsuite_member_dim_type2.sql')

    type2_cs_membership = readfileasstring(cwd + '/sql/membership/update_connectsuite_membership_dim_type2.sql')

    type2_cs_member_customer = readfileasstring(cwd+'/sql/membership/update_membership_customer_dim_type2.sql')

    

    

    global delete_cs_member_md5

    global delete_cs_membership_md5

    global delete_cs_member_customer_md5

    

    

    global remove_cs_member_md5

    global remove_cs_membership_md5

    global remove_cs_membership_customer_md5

    

    

    delete_cs_member_md5 = readfileasstring(cwd + '/sql/membership/delete_member_dim_md5.sql')

    delete_cs_membership_md5 = readfileasstring(cwd + '/sql/membership/delete_membership_dim_md5.sql')

    delete_cs_member_customer_md5 = readfileasstring(cwd + '/sql/membership/delete_membership_customer_dim_md5.sql')

    

    remove_cs_member_md5 = readfileasstring(cwd + '/sql/membership/remove_repeated_member_dim.sql')

    remove_cs_membership_md5 = readfileasstring(cwd + '/sql/membership/remove_repeated_membership_dim.sql')

    remove_cs_membership_customer_md5 = readfileasstring(cwd + '/sql/membership/remove_repeated_member_customer_dim.sql')

    

    

    global load_mdm_stg_sql

    global load_mdm_exceptions_sql    

    global load_mdm_processing_sql

    

    load_mdm_stg_sql=readfileasstring(mdmpath+"/membership/WORK_MDM_MEMBERSHIP.sql")

    load_mdm_exceptions_sql=readfileasstring(mdmpath+"/membership/mdm_exceptions_membership.sql")    

    load_mdm_processing_sql=readfileasstring(mdmpath+"/membership/membership_mdm_processing.sql")

    

    

    

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

        required=True,

        help= ('Incremental Data Pull Filter date')

        )



    parser.add_argument(

        '--inputdate',

        required=False,

        help= ('airflow execution date')

        )

        

    args = parser.parse_args()   

        

    main(args.config,

         args.productconfig,

         args.env,

         args.connectionprefix,

         args.incrementaldate,

         args.inputdate)   

