'''

Created on June 6, 2019

@author: Prerna Anand

'''

from datetime import datetime, timedelta

from datetime import datetime

import os

import argparse

import base64

from google.cloud import bigquery

from google.cloud import storage

import logging



now = datetime.now()



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"



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

    current_date=datetime.today() - timedelta(days=1)

    print(current_date)

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    for blob in bucket.list_blobs(prefix='current/'):

        filename=blob.name.split('/')[0]+'/'+blob.name.split('/')[1]

        flag=1

        if  'Chargeback_'+current_date.strftime("%Y%m%d")+'.csv' in filename:

            filename1=filename.split('/')[1]

            stg_table_name='WORK_EXTERNAL_WNS_CHARGEBACK_PAYMENTS'

            main_table_name='EXTERNAL_WNS_CHARGEBACK_PAYMENTS'

            sql_file='insert_wns_chargeback_payments.sql'

            skip_lead_rows='1'

            file_date=current_date.strftime("%Y-%m-%d")

            

            print("EXTERNAL_WNS_CHARGEBACK_PAYMENTS loaded") 

        elif  'Groupmembership_'+current_date.strftime("%Y%m%d")+'.csv' in filename:

            stg_table_name='WORK_EXTERNAL_GROUPMEMBERSHIP_PAYMENTS'

            main_table_name='EXTERNAL_GROUPMEMBERSHIP_PAYMENTS'      

            sql_file='insert_groupmembership_payments.sql'          

            skip_lead_rows='1'  

            file_date=current_date.strftime("%Y-%m-%d")   

            filename1=filename.split('/')[1]   

            print('EXTERNAL_GROUPMEMBERSHIP_PAYMENTS loaded') 

        elif  'INS_to_MBR_Splits_'+current_date.strftime("%Y%m%d")+'.csv' in filename:

            stg_table_name='WORK_EXTERNAL_INS_TO_MBR_SPLITS_PAYMENTS'

            main_table_name='EXTERNAL_INS_TO_MBR_SPLITS_PAYMENTS'       

            sql_file='insert_ins_to_mbr_splits_payments.sql'        

            skip_lead_rows='1' 

            file_date=current_date.strftime("%Y-%m-%d") 

            filename1=filename.split('/')[1]                         

            print('EXTERNAL_INS_TO_MBR_SPLITS_PAYMENTS loaded')  

        elif  'MBR_to_INS_PymtTrfRqst_'+current_date.strftime("%Y%m%d")+'.csv' in filename:

            stg_table_name='WORK_EXTERNAL_MBR_TO_INS_PYMTRFRQST'

            main_table_name='EXTERNAL_MBR_TO_INS_PYMTRFRQST'        

            sql_file='insert_mbr_to_ins_pymtrfrqst.sql'    

            skip_lead_rows='1'      

            file_date=current_date.strftime("%Y-%m-%d")    

            filename1=filename.split('/')[1]                                           

            print('EXTERNAL_MBR_TO_INS_PYMTRFRQST loaded') 

        elif  'INS_to_MBR_PymtTrfRqst_'+current_date.strftime("%Y%m%d")+'.csv' in filename:            

            stg_table_name='WORK_EXTERNAL_INS_TO_MBR_PYMTRFRQST'

            main_table_name='EXTERNAL_INS_TO_MBR_PYMTRFRQST '    

            sql_file='insert_ins_to_mbr_pymtrfrqst.sql'       

            skip_lead_rows='1' 

            file_date=current_date.strftime("%Y-%m-%d") 

            filename1=filename.split('/')[1]                                                                    

            print('EXTERNAL_INS_TO_MBR_PYMTRFRQST loaded')

        elif  'WD_check_refund_'+current_date.strftime("%Y%m%d")+'.csv' in filename:

            stg_table_name='WORK_EXTERNAL_WD_CHECK_REFUND_PAYMENTS'

            main_table_name='EXTERNAL_WD_CHECK_REFUND_PAYMENTS' 

            sql_file='insert_wd_check_refund_payments.sql'     

            skip_lead_rows='6'   

            file_date=current_date.strftime("%Y-%m-%d")

            filename1=filename.split('/')[1]                                                                                            

            print('EXTERNAL_WD_CHECK_REFUND_PAYMENTS loaded') 

        elif  'NSF222_'+current_date.strftime("%Y%m%d")+'.csv' in filename:

            stg_table_name='WORK_EXTERNAL_NSF222_PAYMENTS'

            main_table_name='EXTERNAL_NSF222_PAYMENTS' 

            sql_file='insert_nsf222_payments.sql'     

            skip_lead_rows='1'     

            file_date=current_date.strftime("%Y-%m-%d")  

            filename1=filename.split('/')[1]                                                                                        

            print('EXTERNAL_NSF222_PAYMENTS loaded') 

        elif  'paymode_detail_payments_'+current_date.strftime("%Y%m%d") in filename and '.csv' in filename:

            stg_table_name='WORK_EXTERNAL_PAYMODE_DETAIL_PAYMENTS'

            main_table_name='EXTERNAL_PAYMODE_DETAIL_PAYMENTS' 

            sql_file='insert_paymode_detail_payments.sql'     

            skip_lead_rows='1'      

            file_date=current_date.strftime("%Y-%m-%d")  

            filename1=filename.split('/')[1]                                                                                       

            print('EXTERNAL_PAYMODE_DETAIL_PAYMENTS loaded') 

        elif  'lockbox_payment_detail_'+current_date.strftime("%Y%m%d")+'.csv' in filename:

            stg_table_name='WORK_EXTERNAL_LOCKBOX_PAYMENT_DETAILS'

            main_table_name='EXTERNAL_LOCKBOX_PAYMENT_DETAILS' 

            sql_file='insert_lockbox_payment_details.sql'     

            skip_lead_rows='1'  

            file_date=current_date.strftime("%Y-%m-%d")    

            filename1=filename.split('/')[1]                                                                                       

            print('EXTERNAL_LOCKBOX_PAYMENT_DETAILS loaded') 

        else:

            flag=0 

                                                                                        

        if flag == 1:        

         print(stg_table_name)

         print(main_table_name)        

         loadstgtables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig ext_payments.properties --env " +env+ " --targettable " + stg_table_name + " --filename "+ filename +" --delimiter , --deposition WRITE_TRUNCATE --skiprows "+skip_lead_rows

         os.system(loadstgtables)

         loadmaintables="python "+utilpath+"runSqlFiles_extpayment.py " + "--config config.properties --productconfig ext_payments.properties --env " +env+ " --sqlfile "+ sql_file + " --tablename " + main_table_name +" --filename "+filename1+" --filedate "+file_date         

         os.system(loadmaintables)



            

    logging.info('All files Loaded  ....')

    loadmaintable="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig ext_payments.properties --env " +env+ " --sqlfile insert_ext_src_pymt_transactions.sql --tablename  EXTERNAL_SOURCE_PAYMENT_TRANSACTIONS"      

    os.system(loadmaintable)



              

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
