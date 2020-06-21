import os
import argparse
from google.cloud import bigquery
import logging

cwd = os.path.dirname(os.path.abspath(__file__))
if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"


         
def main(args_config,args_env):
    logging.getLogger().setLevel(logging.INFO)
    global env
    env = args_env
    global config
    config= args_config
    global env_config
    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals()) 
    env_config =readconfig(config,env)
    global jobname
    jobname="load-shellscript"
    logging.info('Job Name is %s',jobname)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file_uat"]


#      
    client = bigquery.Client(project=env_config['projectid'])
    list = ["CUSTOMER_PRODUCT","REFERENCE","OPERATIONAL"]   
    for dataset1 in list :
        source_dataset = client.dataset(dataset1, project="aaa-mwg-dwprod")
        dest_dataset = client.dataset(dataset1, project="aaa-mwg-dwuat")
        
        tables = client.list_tables(source_dataset)
        for table in tables:
            if table.table_id in ["ERS_SERVICE_FACILITY","COMP_INSURANCE_AGENT"]:
                print("cannot move external tables")
            else:    

                source_table_ref = source_dataset.table(table.table_id)
                dest_table_ref = dest_dataset.table(table.table_id)

    
                print(source_table_ref)
                print(dest_table_ref)
            
           
                job_config = bigquery.CopyJobConfig()
                job_config.write_disposition = "WRITE_TRUNCATE"
                job = client.copy_table(
                    source_table_ref,
                    dest_table_ref,
                    job_config=job_config)

                job.result()  
                print(job.result())  
              
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--config',
        required=True,
        help= ('Config file name')
        )
    parser.add_argument(
        '--env',
        required=True,
        help= ('Enviornment to be run dev/test/prod')
        )
    
    args = parser.parse_args()       
 
main(args.config,
     args.env)       