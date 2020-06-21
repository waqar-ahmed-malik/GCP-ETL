'''

Created on JuAugly 14, 2018

@author: Atul Guleria

'''

from datetime import datetime

import os

import argparse

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

    client = storage.Client(env_config['projectid'])

    bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    for blob in bucket.list_blobs(prefix='current/'):

        filename=blob.name.split('/')[0]+'/'+blob.name.split('/')[1]

        flag=1

        if  'PCSI.GBPD.CPCS.COMPUP.'+now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_COMPUP'

            main_table_name='PCOMP_INSURANCE_TRANSACTION'

            sql_file='insert_compup_pcomp.sql'

            skip_lead_rows='0'

            print("1 WORK_PCOMP_COMPUP") 

      #  elif  'PAS_E_EXGPAS_PRCMPS_4002_D.'+now.strftime("%m.%d.%Y") in filename:

        elif  'PAS_E_EXGPAS_PRCMPS_4002_D.' in filename and now.strftime("%Y%m%d") in filename:

            stg_table_name='WORK_PCOMP_EXIGEN_AUTO'

            main_table_name='PCOMP_INSURANCE_TRANSACTION'      

            sql_file='insert_exigen_pcomp.sql'          

            skip_lead_rows='0'        

            print('2 WORK_PCOMP_EXIGEN_AUTO') 

        elif  'CSAALIST.'+now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_FLOOD'

            main_table_name='PCOMP_INSURANCE_TRANSACTION'       

            sql_file='insert_flood_pcomp.sql'        

            skip_lead_rows='1'                           

            print('3 WORK_PCOMP_FLOOD')  

        elif  'QryHomeIncentive' in filename and now.strftime("%m%d%y") in filename:

            stg_table_name='WORK_PCOMP_HOMEINCENTIVE'

            main_table_name='PCOMP_INSURANCE_TRANSACTION'        

            sql_file='insert_homeinc_pcomp.sql'    

            skip_lead_rows='1'                                                     

            print('4 WORK_PCOMP_HOMEINCENTIVE')   

        elif  'CEABLUEC' in filename and now.strftime("%m%d%Y") in filename:            

            stg_table_name='WORK_PCOMP_CEABLUEC'

            main_table_name='PCOMP_INSURANCE_TRANSACTION'    

            sql_file='insert_ceabluec_pcomp.sql'       

            skip_lead_rows='1'                                                                      

            print('5 WORK_PCOMP_CEABLUEC')  

        elif  'PCSI.GBPD.CPCS.XSDLYSTA.'+now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_HOMEOWNERS'

            main_table_name='PCOMP_INSURANCE_TRANSACTION' 

            sql_file='insert_homeown_pcomp.sql'     

            skip_lead_rows='0'                                                                                               

            print('6 WORK_PCOMP_HOMEOWNERS')      

        elif  'HomeIncentiveAuto' in filename and now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_AUTO_AS400'

            main_table_name='PCOMP_AUTO_AS400'         

            sql_file='insert_auto_as400.sql'    

            skip_lead_rows='1'                                                                                                               

            print('7 WORK_PCOMP_AUTO_AS400')   

        elif  'PAS_E_EXGPAS_PRCMPS_4006_D.'+now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_AUTO_MPA_DAILY'

            main_table_name='PCOMP_AUTO_MPA_DAILY'         

            sql_file='insert_auto_mpa_daily.sql'                                                                                                                            

            print('8 WORK_PCOMP_AUTO_MPA_DAILY')     

            skip_lead_rows='0'                 

        elif  'PAS_E_EXGPAS_PRCMPS_4218_D.'+now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_HO_LEGACY'

            main_table_name='PCOMP_HO_LEGACY'        

            sql_file='insert_ho_legacy.sql'       

            skip_lead_rows='0'                                                                                                                                           

            print('9 WORK_PCOMP_HO_LEGACY')     

        elif  'HomeIncentiveHome' in filename and now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_HO_HISTORY'

            main_table_name='PCOMP_HO_HISTORY'     

            sql_file='insert_ho_history.sql'   

            skip_lead_rows='1'                                                                                                                                                                  

            print('10 WORK_PCOMP_HO_HISTORY')   

        elif  'PCSI.GBPD.CPCS.XSDLYSTB.' in filename and now.strftime("%m.%d.%Y") in filename:

            stg_table_name='WORK_PCOMP_HO_LEGACY_XSDLYSTB'

            main_table_name='PCOMP_HO_LEGACY'            

            sql_file='insert_ho_legacythru_xsdlystb.sql' 

            skip_lead_rows='1'                                                                                                                                                                                

            print('11 WORK_PCOMP_HO_LEGACY_XSDLYSTB')                                                                               

        else:

            flag=0        

        if flag == 1:        

         print(stg_table_name)

         print(main_table_name)        

         loadstgtables="python "+utilpath+"load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig pcomp.properties --env " +env+ " --targettable " + stg_table_name + " --filename "+ filename +" --delimiter , --deposition WRITE_TRUNCATE --skiprows "+skip_lead_rows

         os.system(loadstgtables)

         loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig pcomp.properties --env " +env+ " --sqlfile "+ sql_file + " --tablename " + main_table_name         

         os.system(loadmaintables)

         if stg_table_name == 'WORK_PCOMP_HO_HISTORY':

           loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig pcomp.properties --env " +env+ " --sqlfile insert_ho_2k8_reclassify.sql --tablename PCOMP_HO_2K8_RECLASSIFY" 

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





    

    

    







    



       
