'''
Created on march 25, 2019
@author: Ramneek kaur
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
          database_user=env_config[connectionprefix+'_database_user']
#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')          database_password = env_config[connectionprefix + '_database_password']          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')
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
          query = inquery.replace('v_incr_date',incrementaldate).replace('jobrunid',str(jobrunid)).replace('jobname', jobname).replace('max_date',maxdateforpos)
          logging.info('Query is %s',query)
          logging.info('Query submitted to SqlServer Database')

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
      
          
def runbqsql1(dummy,inputquery):
          client = bigquery.Client(project=env_config['projectid'])
          query=inputquery
          query_job = client.query(query)
          results = query_job.result()
          print(results)
          for row in results:
              global maxdateforpos
              maxdateforpos = str(row[0])
          return maxdateforpos                  
                
      
class runbqsqloop(beam.DoFn):
    def process(self, context, inputquery):
          client = bigquery.Client(project=env_config['projectid'])
          inputquerysplit=inputquery.split(';')
          for inputsql in inputquerysplit:
            query=inputsql
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
                     '--worker_machine_type', "n1-standard-2",
                     '--extra_package', env_config['extra_package']
                    ]
   
    
    try:
        collections = []
        pcoll = beam.Pipeline(argv=pipeline_args)
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])
        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())
        
        
        pos_cus_payments_date_get=(dummy_env | 'Get date for POS customer' >>  beam.Map(runbqsql1,pos_customer_payment_dim_date))
        pos_cus_payments = (pos_cus_payments_date_get | 'POS CUSTOMER PAYMENTS' >>  beam.ParDo(readfromssql(),landing_pos_cus_payments,'WORK_POS_CUSTOMER_PAYMENTS'))
        pos_cus_payments_dim=(pos_cus_payments | 'POS CUSTOMER PAYMENT Dim..' >>  beam.ParDo(runbqsql(),merge_pos_customer_payment_dim))
        
        
        pos_cus_reciept_line_date_get =   (dummy_env | 'Get CUSTOMER RECIEPT LINE ITEMS ' >>  beam.Map(runbqsql1,pos_cus_reciept_line_items_dim_date))
        pos_cus_reciept_line_items = (pos_cus_reciept_line_date_get | 'POS CUSTOMER RECIEPT LINE ITEMS' >>  beam.ParDo(readfromssql(),landing_pos_cus_reciept_line_items,'WORK_POS_CUSTOMER_RECIEPT_LINE_ITEMS'))
        pos_cus_reciept_line_items_dim=(pos_cus_reciept_line_items | 'POS CUSTOMER RECIEPT LINE ITEMS Dim..' >>  beam.ParDo(runbqsql(),merge_pos_cus_reciept_line_items_dim))
        
        
        pos_cus_cc_payments_date_get=(dummy_env | 'Get date for POS CCcustomer' >>  beam.Map(runbqsql1,pos_customer_cc_payment_dim_date))
        pos_cus_cc_payments = (pos_cus_cc_payments_date_get | 'POS CUSTOMER CC PAYMENTS' >>  beam.ParDo(readfromssql(),landing_pos_cus_cc_payments,'WORK_POS_CUST_CC_PYMTS'))
        pos_cus_cc_payments_dim=(pos_cus_cc_payments | 'POS CUSTOMER CC PAYMENT Dim..' >>  beam.ParDo(runbqsql(),merge_pos_customer_cc_payment_dim))
        
        
        pos_cus_pymt_date_get=(dummy_env | 'Get date for POS Cus Pymt' >>  beam.Map(runbqsql1, pos_cus_pymt_dim_date))
        pos_cus_pymt = ( pos_cus_pymt_date_get | 'POS  Cus Pymt' >>  beam.ParDo(readfromssql(),landing_pos_cus_pymt,'WORK_POS_CUS_PYMT'))
        pos_cus_pymt_dim=( pos_cus_pymt | 'POS  Cus Pymt Dim..' >>  beam.ParDo(runbqsql(),merge_pos_cus_pymt_dim))
        
        
        pos_inv_sku_date_get=(dummy_env | 'Get date for POS inv_sku' >>  beam.Map(runbqsql1, pos_inv_sku_dim_date))
        pos_inv_sku = ( pos_inv_sku_date_get | 'POS inv_sku' >>  beam.ParDo(readfromssql(),landing_pos_inv_sku,'WORK_POS_INV_SKU'))
        pos_inv_sku_dim=( pos_inv_sku | 'POS inv_sku..' >>  beam.ParDo(runbqsql(),merge_pos_inv_sku_dim))
        
        
        
        
        collections.append(pos_cus_payments_dim)
        collections.append(pos_cus_reciept_line_items_dim)
        
        flatten=(collections | 'Mart tables of POS loaded' >> beam.Flatten())
        flatten_globally= flatten | 'Dummy : Flatten Globally Mart tables of POS loaded' >> beam.CombineGlobally(any)
        
        
        (flatten_globally | ' POS BANK DEPOSIT' >>  beam.ParDo(runbqsql(),load_pos_bank_deposit)
        | 'POS TENDER PICKUP' >>  beam.ParDo(runbqsql(),load_pos_tender_pickup)
        | 'POS TENDER SUMMARY' >>  beam.ParDo(runbqsql(),load_pos_tender_summary)
        | 'POS TRANSACTION DETAIL' >>  beam.ParDo(runbqsql(),load_pos_transaction_detail)
        |'POS INV CAT' >>  beam.ParDo(readfromssql(),landing_pos_inv_cat,'WORK_POS_INV_CAT')
        |'POS INV CAT Dim..' >>  beam.ParDo(runbqsql(),merge_pos_inv_cat_dim))
        
         
    
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
    jobname="load-pos-landing-"+time.strftime("%Y%m%d")
    global inputdate
    inputdate = args_inputdate
    logging.info('Job Name is %s',jobname)
    
    global landing_pos_cus_payments
    global landing_pos_cus_reciept_line_items
    
    landing_pos_cus_payments = readfileasstring(cwd+ '/sql/pos/work_pos_customer_payments_load.sql')
    landing_pos_cus_reciept_line_items = readfileasstring(cwd+ '/sql/pos/work_pos_customer_reciept_line_item_load.sql')
    
    global pos_customer_payment_dim_date
    global pos_cus_reciept_line_items_dim_date
    
    pos_customer_payment_dim_date = readfileasstring(cwd+ '/sql/pos/pos_customer_payments_date.sql')
    pos_cus_reciept_line_items_dim_date = readfileasstring(cwd+ '/sql/pos/pos_customer_reciept_line_date.sql')
    
    
    
    global merge_pos_customer_payment_dim
    global merge_pos_cus_reciept_line_items_dim
    global load_pos_bank_deposit
    global load_pos_tender_pickup
    global load_pos_tender_summary
    global load_pos_transaction_detail
    
    merge_pos_customer_payment_dim=readfileasstring(cwd+'/sql/pos/pos_customer_payments_load.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_pos_cus_reciept_line_items_dim=readfileasstring(cwd+'/sql/pos/pos_customer_reciept_line_item_load.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    load_pos_bank_deposit=readfileasstring(cwd+'/sql/pos/pos_bank_deposit.sql')
    load_pos_tender_pickup=readfileasstring(cwd+'/sql/pos/pos_tender_pickup.sql')
    load_pos_tender_summary=readfileasstring(cwd+'/sql/pos/pos_tender_summary.sql')
    load_pos_transaction_detail=readfileasstring(cwd+'/sql/pos/pos_transaction_detail.sql')
    
    
    global pos_customer_cc_payment_dim_date
    pos_customer_cc_payment_dim_date=readfileasstring(cwd+'/sql/pos/pos_customer_cc_date.sql')
    global landing_pos_cus_cc_payments
    landing_pos_cus_cc_payments=readfileasstring(cwd+'/sql/pos/work_pos_cust_cc_pymt.sql')
    global merge_pos_customer_cc_payment_dim
    merge_pos_customer_cc_payment_dim=readfileasstring(cwd+'/sql/pos/pos_customer_cc_payments.sql')

    global pos_cus_pymt_dim_date
    pos_cus_pymt_dim_date=readfileasstring(cwd+'/sql/pos/pos_cust_pymt_date.sql')
    global landing_pos_cus_pymt
    landing_pos_cus_pymt=readfileasstring(cwd+'/sql/pos/work_pos_cust_pymt.sql')
    global merge_pos_cus_pymt_dim
    merge_pos_cus_pymt_dim=readfileasstring(cwd+'/sql/pos/pos_cust_pymt.sql')

    global pos_inv_sku_dim_date
    pos_inv_sku_dim_date=readfileasstring(cwd+'/sql/pos/pos_inv_sku_date.sql')
    global landing_pos_inv_sku
    landing_pos_inv_sku=readfileasstring(cwd+'/sql/pos/work_pos_inv_sku.sql')
    global merge_pos_inv_sku_dim
    merge_pos_inv_sku_dim=readfileasstring(cwd+'/sql/pos/pos_inv_sku.sql')
 
    global landing_pos_inv_cat
    landing_pos_inv_cat=readfileasstring(cwd+'/sql/pos/work_pos_inv_cat.sql')
    global merge_pos_inv_cat_dim
    merge_pos_inv_cat_dim=readfileasstring(cwd+'/sql/pos/pos_inv_cat.sql')


    
    
           
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
        required=True,
        help= ('Incremental Data Pull Filter date')
        )
        
    args = parser.parse_args()   
        
    main(args.config,
         args.productconfig,
         args.env,
         args.connectionprefix,
         args.incrementaldate,
         args.inputdate)    
