'''
Created on JuAugly 14, 2018
@author: Atul Guleria
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
    msipath = cwd[:folderup]+"\\msi\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
    msipath = cwd[:folderup]+"/msi/"    

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
    folder = ["commlog", "skipfile", "napa", "erssurvey", "msiers", "mactriptik", "vendnovationkiosk"]
    bucket = ["msi-results-msi", "napatracs--msi", "mac-transactions-msi", "vendnovation-transactions-msi", "dsra-transactions-msi", "msi-skip-msi"]
    for x in folder:
     for blob in bucket.list_blobs(prefix=x+'/current/'):
        filename=blob.name.split('/')[0]+'/'+blob.name.split('/')[1]+'/'+blob.name.split('/')[2]
        file="gs://"+product_config['bucket']+"/"+filename
#         print filename
        flag=1
        if  'ers_export_'+now.strftime("%m_%d_%Y") in filename and x is 'msiers':
            stg_table_name='WORK_MSI_ERS'
            skip_lead_rows='1'
            delimiter='^|'
            print("1 WORK_MSI_ERS")     
            print(file)          
        elif  'VENDNOVATION_KIOSK_INPUT.'+now.strftime("%m.%d.%Y") in filename and x is 'vendnovationkiosk':
            stg_table_name='WORK_VENDNOVATION_KIOSK'
            skip_lead_rows='6'
            delimiter=','
            print("2 WORK_VENDNOVATION_KIOSK")       
        elif  'Export_'+now.strftime("%m_%d_%Y") in filename and x is 'erssurvey':
            stg_table_name='WORK_ERS_SURVEY'
            skip_lead_rows='0'
            delimiter='^|'
            print("3 WORK_ERS_SURVEY")     
        elif  'Report04_'+now.strftime("%m_%d_%Y") in filename and x is 'mactriptik':
            stg_table_name='WORK_MAC_TRIPTIK'
            skip_lead_rows='0'
            delimiter='^|'
            print("4 WORK_MAC_TRIPTIK")            
        elif  'NapaTracs.'+now.strftime("%m.%d.%Y") in filename and x is 'napa':
            stg_table_name='WORK_NAPA'
            skip_lead_rows='1'
            delimiter='^|'
            print("5 WORK_NAPA")           
        elif  'Skip_File_'+now.strftime("%m_%d_%Y") in filename and x is 'skipfile':
            stg_table_name='WORK_SKIP_FILE_MEM_NUM'
            skip_lead_rows='1'
            print("6 WORK_SKIP_FILE_MEM_NUM")                
        elif  x is 'commlog' and now.strftime("%m.%d.%Y") in filename:
            print(filename)
            downloaded_blob = blob.download_as_string()
            stg_table_name='WORK_COMMUNICATION_LOG'                   
            if downloaded_blob.find('|',16,20)>0:
             skip_lead_rows='1'
             delimiter='^|'
             print("delimiter is pipe")
            elif downloaded_blob.find(',',16,20)>0:
             skip_lead_rows='1'         
             delimiter=','
             print("delimiter is comma")             
            elif downloaded_blob.find('\t',16,20)>0:
             skip_lead_rows='1'         
             delimiter='\\t'
             print("delimiter is tab")                     
            print("7 WORK_COMMUNICATION_LOG")                                                                                                    
        else:
            flag=0        
        if flag == 1:        
         print(stg_table_name)
         if stg_table_name == 'WORK_SKIP_FILE_MEM_NUM': 
          loadwrktables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig msi.properties --env "+env+" --targettable "+stg_table_name+" --filename "+ filename +" --delimiter \\t --deposition WRITE_APPEND --skiprows "+skip_lead_rows
          os.system(loadwrktables)
         elif stg_table_name == 'WORK_COMMUNICATION_LOG' and delimiter=='\\t': 
          loadwrktables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig msi.properties --env "+env+" --targettable WORK_COMM_LOG_TAB --filename "+ filename +" --delimiter \\t --deposition WRITE_TRUNCATE --skiprows "+skip_lead_rows
          os.system(loadwrktables) 
          loadtables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig msi.properties --env "+env+" --sqlfile insert_work_communication_log.sql --tablename WORK_COMMUNICATION_LOG"
          os.system(loadtables)                             
         else:    
           loadwrktables="python "+msipath+"load_msi_to_bigquery_landing_dataflow.py " + "--config config.properties --productconfig msi.properties --env " +env+ " --input "+ '"'+file+'"' +" --separator "+delimiter+" --stripheader "+skip_lead_rows+" --stripdelim 0  --addaudit 1   --output "+stg_table_name+"  --writeDeposition WRITE_APPEND"
           os.system(loadwrktables)
         
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


    
    
    



    

       