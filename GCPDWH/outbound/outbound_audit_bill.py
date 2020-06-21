from time import gmtime, strftime
from google.cloud import bigquery
import argparse
import logging
from google.cloud import storage
from google.cloud.storage import Blob
from oauth2client.client import GoogleCredentials
import os
import sys

cwd = os.path.dirname(os.path.abspath(__file__))
#Setting path for folders
if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    sql_query_folder = cwd+"\\sql\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
    sql_query_folder = cwd+"/sql/"
    
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

 destination_uri = "gs://dw-"+env+"-outbound/paramfiles/{}".format(table_name+'.csv') 
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

def main(args_config,args_productconfig,args_env,args_auditsql,args_auditfile):
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
    global audit_file
    audit_file = args_auditfile
    global product_config
    product_config = readconfig(productconfig,env)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
    global audit_sql
    audit_sql = readfileasstring(sql_query_folder+args_auditsql)    
    df_from_bq(audit_sql,audit_file+'_AUDIT')
        
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket('dw-'+env+'-outbound')

    blob_audit = Blob('paramfiles/result_'+audit_file+'_AUDIT.csv', bucket)
    print("blob name is "+blob_audit.name)
    downloaded_blob_audit = blob_audit.download_as_string()
    percent_audit =downloaded_blob_audit[8:13]
    print("Percentage difference is "+percent_audit) 

    print("percent_audit is")
    print(percent_audit)
    if float(percent_audit) >10.0:
        sys.exit(1)
                        
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
        '--auditsql',
        required=True,
        help= ('auditsql to be run')
        )
    parser.add_argument(
        '--auditfile',
        required=True,
        help= ('auditfile')
        )
            
    args = parser.parse_args()       
 
main(args.config,
     args.productconfig,
     args.env,
     args.auditsql,
     args.auditfile)       
