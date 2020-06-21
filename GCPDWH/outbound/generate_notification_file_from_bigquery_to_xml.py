'''
Created on April 4, 2019
Created By: Atul Guleria
This script takes SQL query as argument and stores the SQL result to a XML file in a bucket
Arguments:
    config (environment configuration file)
    productconfig (product configuration file)
    env (dev test or prod)
    detailsqlfile (sql file to be placed at GCPDWH\outbound like sql_file.sql)
    auditsqlfile (audit sql file to be placed at GCPDWH\outbound like sql_file2.sql)
    filename (output xml file name to be generated like abc.xml)
    bucketname (output bucket name)
    foldername (output bucket folder name) 
'''

from time import gmtime, strftime
import argparse
import logging
import os
from datetime import datetime, timedelta
from google.cloud import storage
import pandas
import xml.etree.cElementTree as ET
import time
import numpy as np
from lxml import etree
import zipfile
import shutil
from os import path

now = datetime.now()
cwd = os.path.dirname(os.path.abspath(__file__))
#Setting path for folders
if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    sql_query_folder = cwd+"\\sql\\"
    xml_folder = cwd+"\\xml_data\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"
    sql_query_folder = cwd+"/sql/"
    xml_folder = cwd+"/xml_data/"
#Function to read sql text from file
def readfileasstring(sqlfile):     
     """Read any text file and return text as a string. This function is used to read .sql and .schema files""" 
     with open (sqlfile, "r") as file:
         sqltext=file.read().strip("\n").strip("\r")
     return sqltext
 
#Function to upload file to the bucket 
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name)))     
    
def run():
    try:        

        #Removing XML file from system
        for the_file in os.listdir(xml_folder+foldername):
            file_path = os.path.join(xml_folder+foldername, the_file)
            if os.path.isfile(file_path):
                os.unlink(file_path)
                logging.info('Old file removed from system ....')
                        
        #Reading all detail records from SQL in pandas Dataframe
        df_detail_stg= pandas.read_gbq(readfileasstring(sql_query_folder+detail_sql_file), project_id=env_config['projectid'], dialect='standard')
        df_detail= df_detail_stg.replace(np.nan, '', regex=True)
        #Reading audit data from audit SQL in pandas Dataframe
        df_audit= pandas.read_gbq(readfileasstring(sql_query_folder+audit_sql_file), project_id=env_config['projectid'], dialect='standard') 
               
        i = 0
        #Creating root elements and record tags for XML
        root = ET.Element("aaa_ncnu_notification")
        
        #Adding audit data
        audit = ET.SubElement(root, "audit_fields")
        ET.SubElement(audit, 'run_id').text = str(jobrunid) 
        ET.SubElement(audit, 'processing_date').text = str(time.strftime("%Y-%m-%d")) 
        ET.SubElement(audit, 'detail_record_count').text = str(len(df_detail.index)) 
        for (col,c) in zip(df_audit.columns,list(df_audit)):
            ET.SubElement(audit, c).text = str(df_audit[col][i]) 
            
        #Adding detail records data
        doc = ET.SubElement(root, "detail_records")
        #Iterating through all rows in Dataframe
        while (i < len(df_detail.index)):     
            rows = ET.SubElement(doc, "MEMBERSHIP")         
        #Creating all the subelements as rows and columns from DStaframe
            for (col,c) in zip(df_detail.columns,list(df_detail)):   
                ET.SubElement(rows, c).text = str(df_detail[col][i])                                                                             
            i = i+1    
        tree = ET.ElementTree(root)         
        tree.write(xml_folder+foldername+"/"+file_name)   
        tree = etree.parse(xml_folder+foldername+"/"+file_name)
        root = tree.getroot()

        for elem in root.iter('*'):
            if elem.text is not None:
                elem.text = elem.text.strip()

        with open(xml_folder+foldername+"/"+file_name, 'wb') as f:
            f.write(etree.tostring(root))        
            
        zf = zipfile.ZipFile("%s.zip" % (xml_folder+foldername+"/"+file_name), "w", zipfile.ZIP_DEFLATED)
        zf.write(xml_folder+foldername+"/"+file_name, file_name)
        #Calling function to upload file to the bucket
        upload_blob(bucketname, xml_folder+foldername+"/"+file_name+'.zip', foldername+'/current/'+file_name+'.zip')
        
        logging.info('File loaded to bucket....') 
        
    except:
        logging.exception('Failed to create XML file')
        raise    
            
def main(args_config,args_productconfig,args_env,args_detailsqlfile,args_auditsqlfile,args_filename,args_bucketname,args_foldername):
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
    global detail_sql_file
    detail_sql_file = args_detailsqlfile
    global audit_sql_file
    audit_sql_file = args_auditsqlfile 
    global file_name
    file_name = args_filename      
    global bucketname
    bucketname = args_bucketname
    global foldername
    foldername = args_foldername    
    global jobrunid
    jobrunid=os.getpid()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config["service_account_key_file"]
    logging.info('Started the process  ....')
    
    run() 
              
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
        '--detailsqlfile',
        required=True,
        help= ('File containing SQL query')
        )    
    parser.add_argument(
        '--auditsqlfile',
        required=True,
        help= ('File containing SQL query')
        )
    parser.add_argument(
        '--filename',
        required=True,
        help= ('Output XML file name')
        )    
    parser.add_argument(
        '--bucketname',
        required=True,
        help= ('Output bucket name')
        )    
    parser.add_argument(
        '--foldername',
        required=True,
        help= ('Output foldername name')
        )    
    parser.add_argument(
        '--hierarchyflag',
        required=False,
        help= ('Multiple level children xml')
        )    
                
    args = parser.parse_args()       
 
main(args.config,
     args.productconfig,
     args.env,
     args.detailsqlfile,
     args.auditsqlfile,
     args.filename,
     args.bucketname,
     args.foldername
     )       