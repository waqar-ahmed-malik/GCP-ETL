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

    ctspath = cwd[:folderup]+"\\cts\\"

  

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    ctspath = cwd[:folderup]+"/cts/"    



def main(args_config,args_productconfig,args_env,args_incrementaldate):

    logging.getLogger().setLevel(logging.INFO)

    global env

    env = args_env

    global incrementaldate

    incrementaldate = args_incrementaldate

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



    logging.info('Started Loading Files  ....')

    tablename = ["CTS_COMPLAINT_DIM", "CTS_COMPLAINT_HISTORY_DIM", "CTS_INCIDENT_FACT", "CTS_INCIDENT_HISTORY_FACT", "CTS_INCIDENT_ROUTING_LIST_FACT"]

    for x in tablename:

        flag=1

        if x in 'CTS_COMPLAINT_DIM':

            stgsql='insert_work_cts_complaint_dim.sql'

            mainsql='merge_cts_complaint_dim.sql'

            print("1 CTS_COMPLAINT_DIM")     

            print(file)          

        elif x in 'CTS_COMPLAINT_HISTORY_DIM':

            stgsql='insert_work_cts_complaint_history_dim.sql'

            mainsql='merge_cts_complaint_history_dim.sql'

            print("2 CTS_COMPLAINT_HISTORY_DIM")     

            print(file)       

        elif x in 'CTS_INCIDENT_FACT':

            stgsql='insert_work_cts_incident_fact.sql'

            mainsql='merge_cts_incident_fact.sql'

            print("3 CTS_INCIDENT_FACT")     

            print(file)       

        elif x in 'CTS_INCIDENT_HISTORY_FACT':

            stgsql='insert_work_cts_incident_history_fact.sql'

            mainsql='merge_cts_incident_history_fact.sql'

            print("4 CTS_INCIDENT_HISTORY_FACT")     

            print(file)      

        elif x in 'CTS_INCIDENT_ROUTING_LIST_FACT':

            stgsql='insert_work_cts_incident_routing_list_fact.sql'

            mainsql='merge_cts_incident_routing_list_fact.sql'

            print("5 CTS_INCIDENT_ROUTING_LIST_FACT")     

            print(file)                                                                                                                                      

        else:

            flag=0        

        if flag == 1:        

         loadwrktables="python "+utilpath+"load_from_oracle_to_bigquery.py " + "--config config.properties --productconfig cts.properties --env "+env+" --targettable WORK_"+ x +" --sqlquery "+ stgsql +" --connectionprefix d3 --incrementaldate "+incrementaldate

         os.system(loadwrktables)    

         loadmaintables="python "+utilpath+"runSqlFiles.py " + "--config config.properties --productconfig cts.properties --env " +env+ " --sqlfile "+ mainsql + " --tablename " + x         

         os.system(loadmaintables)                                    

        else:    

           print('nothing')

         

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

    parser.add_argument(

        '--incrementaldate',

        required=True,

        help= ('Enviornment to be run dev/test/prod')

        )    

    

    args = parser.parse_args()       

 

main(args.config,

     args.productconfig,

     args.env,

     args.incrementaldate)       





    

    

    







    



       
