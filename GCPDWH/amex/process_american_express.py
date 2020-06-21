'''

Created on Aug 17, 2018

@author: Prerna Anand

'''

from datetime import datetime,timedelta

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging



now = datetime.now()- timedelta(days=1)

print(now)



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    homepath = cwd[:folderup]+"\\amex\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    homepath = cwd[:folderup]+"/amex/"



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



def main(args_config,args_productconfig,args_env,args_inputdate):

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

    

    global inputdate

    inputdate=args_inputdate

    jobname="load_"+product_config['productname']

    logging.info('Job Name is %s',jobname)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    for blob in bucket.list_blobs(prefix='amex-payment-transactions/current/AAANCNUNAA61427.GRRCN.'):

        gs = "gs://"

        filename=gs+product_config['bucket']+'/'+blob.name.split('/')[0]+'/'+blob.name.split('/')[1]+'/'+blob.name.split('/')[2]

        print(filename)

        today = now.strftime("%y%m%d")

        print(today)        

        flag=1

        if 'AAANCNUNAA61427.GRRCN.' in filename and inputdate in filename :

            logging.info('Truncating work tables  ....')

            sql=readfileasstring(cwd+'/sql/delete_amex_work_tables.sql').split(';')

            for inputsql in sql:

                logging.info('Executing query...%s',inputsql)

                run_query(inputsql) 

                logging.info('Query completed!...')

            logging.info('Work tables truncated...')  

            

            loadamextables="python "+homepath+"landing_amex_files_to_bigquery_work_tables.py "+ "--config config.properties --productconfig amex.properties --env "+env+" --input " + filename +" --separator , --stripheader 1 --stripdelim 0 --addaudit 1 --writeDeposition WRITE_TRUNCATE"

            os.system(loadamextables)

             

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_adj.sql --tablename AMEX_PAYMENT_ADJUSTMENT"       

            os.system(loadmaintable)                                    

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_chrg_bck.sql --tablename AMEX_PAYMENT_CHARGEBACK"        

            os.system(loadmaintable)   

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_fee_nd_revenu.sql --tablename AMEX_PAYMENT_FEES_AND_REVENUES"     

            os.system(loadmaintable)   

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_subm.sql --tablename AMEX_PAYMENT_SUBMISSION"   

            os.system(loadmaintable)                     

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_summary.sql --tablename AMEX_PAYMENT_SUMMARY"   

            os.system(loadmaintable)                                    

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_tax.sql --tablename AMEX_PAYMENT_TAX"   

            os.system(loadmaintable)   

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_trans_pricing.sql --tablename AMEX_PAYMENT_TRANSACTION_PRICING"    

            os.system(loadmaintable)   

            loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig amex.properties --env  "+env+" --sqlfile insert_amex_payment_trans.sql --tablename AMEX_PAYMENT_TRANSACTION"       

            os.system(loadmaintable)                        

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

    parser.add_argument(

        '--inputdate',

        required=True,

        help= ('Enviornment to be run dev/test/prod')

        )    

    args = parser.parse_args()       

 

main(args.config,

     args.productconfig,

     args.env,

    args.inputdate)       
