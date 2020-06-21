'''
Created on April 10 ,2019
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
          os.system('gsutil cp '+product_config['stagingbucket']+'/jconn4.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )
          logging.info('Jar copied to Instance..')
          logging.info('Java Libraries copied to Instance..')
          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')
          logging.info('Enviornment Variable set.')
          return list("1")


class readfromsybase(beam.DoFn):
      def process(self, context, inquery,targettable):
          database_user=env_config[connectionprefix+'_database_user']
#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')          database_password = env_config[connectionprefix + '_database_password']          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')
          database_host=env_config[connectionprefix+'_database_host']
          database_database=env_config[connectionprefix+'_database']
          database_port=env_config[connectionprefix+'_database_port']


          jclassname = "com.sybase.jdbc4.jdbc.SybDriver"
          url = ("jdbc:sybase:Tds:" + database_host + ':' + database_port + '/' + database_database + '?ENABLE_SSL=true&SSL_TRUST_ALL_CERTS=true;')
          jars = ["/tmp/jconn4.jar"]
          libs = None
          cnx = jaydebeapi.connect(jclassname, url,[database_user,database_password], jars=jars,libs=libs)
          print(cnx)
          print ('Connection Successful')
          logging.info('Connection Successful..')
          cnx.cursor()
          logging.info('Reading Sql Query from the file...')
          query = inquery.replace('jobrunid',str(jobrunid)).replace('jobname', jobname).replace('max_payid',maxidforcomments).replace('v_startdate',inputstartdate).replace('v_enddate',inputenddate)
          logging.info('Query is %s',query)
          logging.info('Query submitted to SqlServer Database')

          logging.info("Started loading Table")
          for chunk in pandas.read_sql(query.replace("v_database_name", database_database), cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=500000):
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



class runbqsql1(beam.DoFn):
    def process(self, context, inputquery):
          client = bigquery.Client(project=env_config['projectid'])
          query=inputquery
          query_job = client.query(query)
          results = query_job.result()
          print(results)
          for row in results:
              global maxidforcomments
              maxidforcomments = str(row[0])
              return maxidforcomments



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


        (dummy_env | 'GLOBALWARE COMMENTS max payid ' >>  beam.ParDo(runbqsql1(), get_bqpayid)
        | 'GLOBALWARE COMMENTS LANDING' >>  beam.ParDo(readfromsybase(),landing_gw_comments,'WORK_GW_COMMENTS')
        | 'GLOBALWARE COMMENTS' >>  beam.ParDo(runbqsql(),insert_globalware_comments))

        (dummy_env | 'GLOBALWARE INVOICE max payid ' >>  beam.ParDo(runbqsql1(), get_bqinvoicepayid)
        | 'GLOBALWARE INVOICE LANDING' >>  beam.ParDo(readfromsybase(),landing_gw_invoice,'WORK_GW_INVOICE')
        | 'GLOBALWARE INVOICE' >>  beam.ParDo(runbqsql(),insert_globalware_invoice))

        (dummy_env | 'GLOBALWARE SEGMENTS max payid ' >>  beam.ParDo(runbqsql1(), get_bqpayidsegmenet)
        | 'GLOBALWARE SEGMENTS LANDING' >>  beam.ParDo(readfromsybase(),landing_gw_invoice,'WORK_GW_INVOICE')
        | 'GLOBALWARE SEGMENTS' >>  beam.ParDo(runbqsql(),insert_globalware_segments)
 
 
        | 'GLOBALWARE GL HEADER LANDING' >>  beam.ParDo(readfromsybase(),landing_gw_gl,'WORK_GW_GL_HEADER')
        | 'GLOBALWARE GL HEADER' >>  beam.ParDo(runbqsql(),insert_globalware_gl)

        | 'GLOBALWARE CASH RECIEPTS LANDING' >>  beam.ParDo(readfromsybase(),landing_gw_cash,'WORK_GW_CASH_RECIPETS')
        | 'GLOBALWARE CASH RECIEPTS' >>  beam.ParDo(runbqsql(),insert_globalware_cash)


        | 'GLOBALWARE PAYMENTS LANDING' >>  beam.ParDo(readfromsybase(),landing_gw_payments,'WORK_GW_PAYMENTS')
        | 'GLOBALWARE PAYMENTS' >>  beam.ParDo(runbqsql(),insert_globalware_payments))



        p = pcoll.run()
        p.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise

def main(args_config,args_productconfig,args_env,args_connectionprefix,args_incrementaldate,args_inputstartdate,args_inputenddate):
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
    global inputstartdate
    inputstartdate = args_inputstartdate
    global inputenddate
    inputenddate = args_inputenddate
    global sqlstring
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    jobname="load-"+product_config['productname']+"-"+"land"+"-"+time.strftime("%Y%m%d")
    logging.info('Job Name is %s',jobname)

    global landing_gw_invoice
    global landing_gw_comments
    global landing_gw_segments
    global landing_gw_gl
    global landing_gw_cash
    global landing_gw_payments



    landing_gw_invoice = readfileasstring(cwd + '/sql/work_gw_invoice.sql')
    landing_gw_comments = readfileasstring(cwd + '/sql/work_gw_comments.sql')
    landing_gw_segments = readfileasstring(cwd + '/sql/work_gw_segments.sql')
    landing_gw_gl = readfileasstring(cwd + '/sql/work_gw_gl.sql')
    landing_gw_cash = readfileasstring(cwd + '/sql/work_gw_cash_reciepts.sql')
    landing_gw_payments = readfileasstring(cwd + '/sql/work_gw_payments.sql')


    global insert_globalware_invoice
    global insert_globalware_comments
    global insert_globalware_segments
    global insert_globalware_gl
    global insert_globalware_cash
    global insert_globalware_payments


    global get_bqpayid
    global get_bqinvoicepayid
    global get_bqpayidsegmenet

    insert_globalware_invoice = readfileasstring(cwd + '/sql/invoice.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    insert_globalware_comments = readfileasstring(cwd + '/sql/comments.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    insert_globalware_segments = readfileasstring(cwd + '/sql/segments.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    insert_globalware_gl = readfileasstring(cwd + '/sql/gl_header.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    insert_globalware_cash = readfileasstring(cwd + '/sql/cash_reciepts.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    insert_globalware_payments = readfileasstring(cwd + '/sql/payments.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    get_bqpayid = readfileasstring(cwd + '/sql/comments_max_payid.sql')
    get_bqinvoicepayid = readfileasstring(cwd + '/sql/invoice_max_payid.sql')
    get_bqpayidsegmenet = readfileasstring(cwd + '/sql/segments_max_payid.sql')



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
        '--inputstartdate',
        required=True,
        help= ('Incremental Data Pull Filter start date')
        )

    parser.add_argument(
        '--inputenddate',
        required=True,
        help= ('Incremental Data Pull Filter end date')
        )

    args = parser.parse_args()

    main(args.config,
         args.productconfig,
         args.env,
         args.connectionprefix,
         args.incrementaldate,
         args.inputstartdate,
         args.inputenddate)