'''
Created on August 30, 2018
@author: Aarzoo Malik
'''
from datetime import datetime
import os
import argparse
from string import  upper
from google.cloud import bigquery
from google.cloud import storage
from string import upper
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
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket(product_config['bucket'])
    logging.info('Bucket name is %d',bucket)
    print(bucket)
    logging.info('Started Loading Files  ....')
    #logging.info('Blob list is %d',bucket.list_blobs(prefix='current/'))
    for blob in bucket.list_blobs(prefix='current/'):
    #    logging.info('Blob name is %d',blob)
        filename=blob.name.split('/')[0]+'/'+blob.name.split('/')[1]
     #   logging.info('File name is %d',filename)
        print(filename)
        
        flag=1
        if  'AllRegistrationsCompleteOverview_'+now.strftime("%Y-%m-%d") in filename:
            stg_table_name='WORK_GIG_REGISTRATIONS_OVERVIEW'
            main_table_name='GIG_REGISTRATIONS_OVERVIEW'
            sql_file='insert_gig_registration_overview.sql'
            skip_lead_rows='1'
            print("1 WORK_GIG_REGISTRATIONS_OVERVIEW") 
        elif  'AppOpensVsReservationVsBookingCountPST_'+now.strftime("%Y-%m-%d") in filename:
            stg_table_name='WORK_GIG_APPOPEN_RESERVATION_BOOKING'
            main_table_name='GIG_APPOPEN_RESERVATION_BOOKING'      
            sql_file='insert_gig_appopen_reservation_booking.sql'          
            skip_lead_rows='1'        
            print('2 WORK_GIG_APPOPEN_RESERVATION_BOOKING') 
        elif  'Events '+now.strftime("%Y%m%d") in filename:
            stg_table_name='WORK_GIG_EVENTS'
            main_table_name='GIG_EVENTS'       
            sql_file='insert_gig_events.sql'        
            skip_lead_rows='1'                           
            print('3 WORK_GIG_EVENTS')  
        elif  'gig_daily_damage_report_' in filename and now.strftime("%Y_%m_%d") in filename:
            stg_table_name='WORK_GIG_DAILY_DAMAGE_REPORT'
            main_table_name='GIG_DAILY_DAMAGE_REPORT'        
            sql_file='insert_gig_daily_damage_report.sql'    
            skip_lead_rows='1'                                                     
            print('4 WORK_GIG_DAILY_DAMAGE_REPORT')   
        elif  'gig_daily_rental_rating_' in filename and now.strftime("%Y_%m_%d") in filename:            
            stg_table_name='WORK_GIG_DAILY_RENTAL_RATING'
            main_table_name='GIG_DAILY_RENTAL_RATING'    
            sql_file='insert_gig_daily_rental_rating.sql'       
            skip_lead_rows='1'                                                                      
            print('5 WORK_GIG_DAILY_RENTAL_RATING')  
        elif  'gig_daily_rental_report_'+now.strftime("%Y_%m_%d") in filename:
            stg_table_name='WORK_GIG_DAILY_RENTAL_REPORT'
            main_table_name='GIG_DAILY_RENTAL_REPORT' 
            sql_file='insert_gig_daily_rental_report.sql'     
            skip_lead_rows='1'                                                                                               
            print('6 WORK_GIG_DAILY_RENTAL_REPORT')      
        elif  'gig_daily_vehicle_history_report_' in filename and now.strftime("%Y_%m_%d") in filename:
            stg_table_name='WORK_GIG_DAILY_VEHICLE_HISTORY_REPORT'
            main_table_name='GIG_DAILY_VEHICLE_HISTORY_REPORT'         
            sql_file='insert_gig_daily_vehicle_history_report.sql'    
            skip_lead_rows='1'                                                                                                               
            print('7 WORK_GIG_DAILY_VEHICLE_HISTORY_REPORT')   
        else:
            flag=0   
            print("flag 0")     
        if flag == 1:        
         print(stg_table_name)
         print(main_table_name)        
         loadstgtables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig gig.properties --env " +env+ " --targettable " + stg_table_name + " --filename "+ filename +" --delimiter , --deposition WRITE_TRUNCATE --skiprows "+skip_lead_rows
         os.system(loadstgtables)
         loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig gig.properties --env " +env+ " --sqlfile "+ sql_file + " --tablename " + main_table_name         
         os.system(loadmaintables)
        
#     ''' Move files to Archive folder '''
#     logging.info("Archiving files...")
#     movefiles= "gsutil mv gs://"+product_config['bucket']+"/current/*"+" gs://"+product_config['bucket']+"/archive/"
#     print movefiles
#     os.system(movefiles)
#     logging.info("Files has been successfully copied from current folder to archive folder..")
              
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
