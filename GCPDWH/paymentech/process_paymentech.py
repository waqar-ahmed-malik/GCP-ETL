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



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    homepath = cwd[:folderup]+"\\paymentech\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    homepath = cwd[:folderup]+"/paymentech/"



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

    for blob in bucket.list_blobs(prefix='current/246206.0000197845'):

        

        gs = "gs://"

        filename=gs+product_config['bucket']+'/'+blob.name.split('/')[0]+'/'+blob.name.split('/')[1]

        print(filename)

        file_id="20"+filename.split('.')[2]

        fileid=datetime.strptime(file_id,"%Y%m%d").strftime("%m%d%Y")

        print(fileid)

        flag=1

        if  'B' in filename and inputdate in fileid and '.dfr_resp'in filename:

            stg_table_name='WORK_CHASE_PAYMENTS'

            main_table_name='CHASE_PAYMENTS'

            sql_file='insert_chase_payments.sql'

            

            print("1 WORK CHASE PAYMENTS") 

            loadpaymentstgtables="python "+homepath+"load_chase_payments_to_bigquery_landing.py " + "--config config.properties --productconfig paymentech.properties --env "+env+" --input " + filename + " --separator , --stripheader 1 --stripdelim 0 --addaudit 1 --output " +stg_table_name + " --writeDeposition WRITE_TRUNCATE"

            os.system(loadpaymentstgtables)

            

            loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig paymentech.properties --env  "+env+" --sqlfile "+ sql_file + " --tablename " + main_table_name         

            os.system(loadmaintables) 

        elif  'A' in filename and inputdate in fileid and '.dfr_resp'in filename:

            stg_table_name='WORK_CHASE_PAYMENT_FAILURES'

            main_table_name='CHASE_PAYMENT_FAILURES'      

            sql_file='insert_chase_payment_failures.sql'          

                   

            print('2 WORK CHASE PAYMENT FAILURES') 

            loadfailurestgtables="python "+homepath+"load_chase_payments_failure_to_bigquery_landing.py " + "--config config.properties --productconfig paymentech.properties --env "+env+" --input " + filename + " --separator , --stripheader 1 --stripdelim 0 --addaudit 1 --output " +stg_table_name + " --writeDeposition WRITE_TRUNCATE"

            os.system(loadfailurestgtables)

           

            loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig paymentech.properties --env "+env+" --sqlfile "+ sql_file + " --tablename " + main_table_name         

            os.system(loadmaintables)            

                                                                                  

        else:

            flag=0        

        if flag == 1:        

         print(stg_table_name)

         print(main_table_name)        

            

         logging.info(stg_table_name+' Loaded  ....')

    



              

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





    

    

    







    



       
