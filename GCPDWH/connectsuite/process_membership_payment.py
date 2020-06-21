'''

Created on Sep 14, 2018

@author: Aarzoo Malik

'''

from datetime import datetime

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import base64

import logging



now = datetime.now()



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    connectsuitepath = cwd[:folderup]+"\\connectsuite\\sql\\membership"

   

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    connectsuitepath = cwd[:folderup]+"/connectsuite/sql/membership"    



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

    client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    targettable = ["CONNECTSUITE_DONOR_DIM", "CONNECTSUITE_ACCOUNTS_PAYABLE", "CONNECTSUITE_JOURNAL_ENTRY", "CONNECTSUITE_CARD_REQUEST_HISTORY", "CONNECTSUITE_E_PAYMENT_BILLING", "CONNECTSUITE_E_PAYMENT_HISTORY", "CONNECTSUITE_MEMBERSHIP_DONOR","CONNECTSUITE_MEMBERSHIP_COST","CONNECTSUITE_MEMBER_SOLICITATION","CONNECTSUITE_BATCH_REJECT","CONNECTSUITE_MEMBERSHIP_PAYMENT"]

    for filename in targettable:

        if  'CONNECTSUITE_DONOR_DIM' in filename:

            main_table_name='CONNECTSUITE_DONOR_DIM'

            sql_file='connectsuite_donor_dim_load.sql'

            print("1 CONNECTSUITE_DONOR_DIM") 

        elif  'CONNECTSUITE_ACCOUNTS_PAYABLE' in filename:

            main_table_name='CONNECTSUITE_ACCOUNTS_PAYABLE'      

            sql_file='connectsuite_accounts_payable_load.sql'                

            print('2 CONNECTSUITE_ACCOUNTS_PAYABLE') 

        elif  'CONNECTSUITE_JOURNAL_ENTRY' in filename:

            main_table_name='CONNECTSUITE_JOURNAL_ENTRY'       

            sql_file='connectsuite_journal_entry_load.sql'                                   

            print('3 CONNECTSUITE_JOURNAL_ENTRY')  

        elif  'CONNECTSUITE_CARD_REQUEST_HISTORY' in filename :

            main_table_name='CONNECTSUITE_CARD_REQUEST_HISTORY'        

            sql_file='connectsuite_card_request_history_load.sql'                                                        

            print('4 CONNECTSUITE_CARD_REQUEST_HISTORY')   

        elif  'CONNECTSUITE_E_PAYMENT_BILLING' in filename:            

            main_table_name='CONNECTSUITE_E_PAYMENT_BILLING'    

            sql_file='connectsuite_e_payment_billing_load.sql'                                                                             

            print('5 CONNECTSUITE_E_PAYMENT_BILLING')  

        elif  'CONNECTSUITE_E_PAYMENT_HISTORY' in filename:

            main_table_name='CONNECTSUITE_E_PAYMENT_HISTORY' 

            sql_file='insert_GIG_DAILY_RENTAL_REPORT.sql'                                                                                                   

            print('6 CONNECTSUITE_E_PAYMENT_HISTORY')      

        elif  'CONNECTSUITE_MEMBERSHIP_DONOR' in filename:

            main_table_name='CONNECTSUITE_MEMBERSHIP_DONOR'         

            sql_file='connectsuite_membership_donor_load.sql'                                                                                        

            print('7 CONNECTSUITE_MEMBERSHIP_DONOR')

        elif  'CONNECTSUITE_MEMBERSHIP_COST' in filename:

            main_table_name='CONNECTSUITE_MEMBERSHIP_COST'         

            sql_file='connectsuite_membership_cost_load.sql'                                                                                         

            print('8 CONNECTSUITE_MEMBERSHIP_COST')			

        elif  'CONNECTSUITE_MEMBER_SOLICITATION' in filename:

            main_table_name='CONNECTSUITE_MEMBER_SOLICITATION'         

            sql_file='connectsuite_member_solicitation_load.sql'                                                                                     

            print('9 CONNECTSUITE_MEMBER_SOLICITATION')			

        elif  'CONNECTSUITE_BATCH_REJECT' in filename:

            main_table_name='CONNECTSUITE_BATCH_REJECT'         

            sql_file='connectsuite_batch_reject_load.sql'                                                                                            

            print('10 CONNECTSUITE_BATCH_REJECT')			

        elif  'CONNECTSUITE_MEMBERSHIP_PAYMENT' in filename:

            main_table_name='CONNECTSUITE_MEMBERSHIP_PAYMENT'         

            sql_file='connectsuite_membership_payment_load.sql'                                                                                                                

            print('11 CONNECTSUITE_MEMBERSHIP_PAYMENT')			

        else:

            flag=0        

        if flag == 1:        

            print(main_table_name)        

            loadstgtables="python "+utilpath+"load_from_mssql_to_bigquery.py" + "--config config.properties --productconfig mssql.properties --env " +env+ " --targettable " +main_table_name+ " --sqlquery " + sql_file +  "--connectionprefix cs"

            os.system(loadstgtables)

        

              

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

