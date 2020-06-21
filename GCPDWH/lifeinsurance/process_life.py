'''

Created on Sep 06, 2019

@author: Maniratnam Patchigolla

'''

from datetime import datetime,timedelta

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging



now = datetime.now() - timedelta(days=1)

print(now.strftime("%m%d%y"))



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



def run_query(inputquery):

     client = bigquery.Client(project=env_config['projectid'])

     query=inputquery

     query_job = client.query(query)

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

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    for blob in bucket.list_blobs(prefix='aaalife/current/'):

        filename=blob.name.split('/')[0]+'/'+blob.name.split('/')[1]+'/'+blob.name.split('/')[2]

 

        flag=1

        print(filename)



        if  'CSAPRDLY' in filename and now.strftime("%m%d%y") in filename:

            

            stg_table_name='WORK_LIFE_INS_CSAPRDLY_DAILY'

            main_table_name='LIFE_INS_CSAPRDLY_DAILY'

            

            if filename[31:33] == '01':

                sql_file='loadcsadailytable_monthlyrun.sql'

            else :

                sql_file='loadcsadailytable.sql'

                 

            logging.info('Truncating LIFE_INS_CSAPRDLY_DAILY table ....')    

            run_query(readfileasstring(cwd+"/sql/delete_csadailytable.sql"))

             

            skip_lead_rows='0'

            print("1 WORK LIFE INS CSAPRDLY DAILY") 

             

        elif  'csaprmth' in filename and now.strftime("%m%d%y") in filename:

            print(filename)

            stg_table_name='WORK_LIFE_INS_CSAPRDLY_MONTHLY'

            main_table_name='LIFE_INS_CSAPRDLY_MONTHLY'      

            sql_file='loadcsaamonthly.sql'          

            skip_lead_rows='0'        

            print('2 WORK LIFE INS CSAPRDLY MONTHLY') 

              

        elif  'AGYPOLEX' in filename:

            stg_table_name='WORK_LIFE_INS_AGYPOLE'

            main_table_name='AAA_LIFE_INS_AGYPOLE'       

            sql_file='loadagypoletable.sql'        

            skip_lead_rows='1'                           

            print('3 WORK LIFE INS AGYPOLE') 



        else:

            flag=0      

        if flag == 1:        

            print(stg_table_name)

            print(main_table_name)   

             

            loadstgtables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig lifeinsurance.properties --env "+ env + " --targettable " + stg_table_name + " --filename "+ filename +" --delimiter , --deposition WRITE_TRUNCATE --skiprows "+skip_lead_rows

            os.system(loadstgtables)

            loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig lifeinsurance.properties --env "+ env + " --sqlfile "+ sql_file + " --tablename " + main_table_name         

            os.system(loadmaintables)         



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

