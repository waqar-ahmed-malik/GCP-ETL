from time import gmtime, strftime
from google.cloud import bigquery
import argparse
import logging
from google.cloud import storage
from google.cloud.storage import Blob
from oauth2client.client import GoogleCredentials
import os

cwd = os.path.dirname(os.path.abspath(__file__))
if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
def readfileasstring(sqlfile):   
    """
    Read any text file and return text as a string. 
    This function is used to read .sql and .schema files
    """ 

    with open (sqlfile, "r") as file:
         sqltext=file.read().strip("\n").strip("\r")
    return sqltext    

def df_from_bq(query,table=None,compute=False):    
 client = bigquery.Client.from_service_account_json(env_config["service_account_key_file"]) #Authentication if BQ using ServiceKey
 project = env_config['projectid']
 table_name = 'result_'+table

 job_config = bigquery.QueryJobConfig()
 table_ref = client.dataset(product_config['datasetname']).table(table_name)
 job_config.destination = table_ref
 job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #Creates the table with query result. Overwrites it if the table exists

 query_job = client.query(
    query,
    location='US', 
    job_config=job_config)
 query_job.result() 
 print(('Query results loaded to table {}'.format(table_ref.path)))

 destination_uri = "gs://dw-prod-ers/paramfiles/{}".format(table_name+'.csv') 
 dataset_ref = client.dataset(product_config['datasetname'], project=project)
 table_ref = dataset_ref.table(table_name)

 extract_job = client.extract_table(
    table_ref,
    destination_uri,
    location='US') 
 extract_job.result() #Extracts results to the GCS


 print(('Query results extracted to GCS: {}'.format(destination_uri)))

 client.delete_table(table_ref) #Deletes table in BQ

 print(('Table {} deleted'.format(table_name)))


 print('Results imported to DD!') 
 return "true"

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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
    df_from_bq('SELECT max(ARCH_DATETIME) as MAX_ARCH_DATE FROM `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_ARCH_CALL` LIMIT 1000','ARCH_DATETIME')

    client = storage.Client(env_config['projectid'])
    bucket = client.bucket('dw-prod-ers')
    blob = Blob('paramfiles/result_ARCH_DATETIME.csv', bucket)
    print("blob name is "+blob.name)
    downloaded_blob = blob.download_as_string()
    date_string =downloaded_blob[14:33]
#     print("date string is "+date_string) 
    incrementaldate_arch =date_string.decode("utf-8")  
     
    print("Incremnetaldate_arch is")
    print(incrementaldate_arch)

    
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
