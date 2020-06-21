from datetime import datetime, timedelta

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging

import sys



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    

global status1    



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

     global currentvalue

     for row in results:

        global currentvalue

        currentvalue = str(row[0])

     return currentvalue   

         

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

    global jobrunid

    jobrunid=os.getpid()

    logging.info('Job Run Id is %d',jobrunid)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]

    

    logging.info('Started queries for comparison') 

    

    flag = 0    

    '''Comparison for Member dim  Table '''

    logging.info('Comparison for Member dim  Table')   

    run_query(readfileasstring(cwd+"/membership/member_dim_compare.sql"))  

    print(currentvalue)

    if int(currentvalue) < 20000 :   

        flag += 1

        print(flag)

        msg = 'Member_dim count inserted in main table is lower than expected average'    

    else:

        msg = ""           

   

    

    '''Comparison for Membership dim  Table'''

    logging.info('Comparison for Membership dim  Table')   

    run_query(readfileasstring(cwd+"/membership/membership_dim_compare.sql"))   

    print(currentvalue)

    if int(currentvalue) < 8000 :   

        flag += 1

        print(flag)

        msg2 = 'Membership_dim count inserted in main table is lower than expected average'        

    else:

        msg2 = ""

    '''Comparison for Membership Customer dim  Table '''

    logging.info('Comparison for Membership Customer dim  Table ')   

    run_query(readfileasstring(cwd+"/membership/customer_dim_compare.sql")) 

    print(currentvalue)   

    if int(currentvalue) < 15000 : 

        flag += 1

        print(flag)

        msg3 = 'customer_dim count inserted in main table is lower than expected average'  

    else:

        msg3 = ""

    '''Comparison for Membership transctions fact  Table '''

    logging.info('Comparison for Membership transctions fact  Table')   

    run_query(readfileasstring(cwd+"/membership/transactions_fact_compare.sql"))   

    print(currentvalue)   

    if currentvalue == "false" :

        flag += 1

        print(flag)

        msg4 = 'transaction_fact count inserted in main table is lower than expected average'    

    else:

        msg4 = ""

        

    print(flag)   

    

    

    if flag > 0:    

        print(msg+msg2+msg3+msg4)

        logging.info('FAILED STATUS UPDATE IN AUDIT TABLE ')   

        run_query(readfileasstring(cwd+"/membership/audit_status_update_s.sql")) 

        logging.info(msg+"**"+msg2+"**"+msg3+"**"+msg4) 

        logging.info("There is a count mismatch in membership tables") 

        sys.exit(1) 

    else:

        logging.info('SUCCESS STATUS UPDATE IN AUDIT TABLE ')   

        run_query(readfileasstring(cwd+"/membership/audit_status_update_f.sql")) 

        print("all table counts are as expected")         

    

      





    

              

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
