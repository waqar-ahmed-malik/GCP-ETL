
import apache_beam as beam
from apache_beam import pvalue
import argparse
import logging
import os
from google.cloud import bigquery
from string import lower
from google.cloud import storage as gstorage
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time
import pandas as pd
import datetime
from pandas._libs.index import date
from numpy import dtype, int64
from builtins import int


current_date = datetime.datetime.today().date()

cwd = os.path.dirname(os.path.abspath(__file__))
print(cwd)

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

print(utilpath)

def process_ivans_file(input,segment):
    
    remote_file_path=cwd+"\\data\\" 
    print(remote_file_path)
    storage_client = gstorage.Client(env_config['projectid'])
    bucket = storage_client.get_bucket(product_config['bucket'])
    blob = bucket.get_blob(input)
    print(input)
    blob.download_to_filename(remote_file_path+input)
    fileid=input[8:16]
    print(fileid) 
    
  
    #PROCESSING OF DATA FOR IE SOURCES
    
    if (segment=='IEA1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,20),(20,30),(30,40),(40,46),(46,56),(56,59),(59,65),(65,75),(75,330)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='A1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
        data.insert(loc=0,column='fileid', value=ffileid)
        data[14]=current_date
        data[15]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEA1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')  
        
    if (segment=='IEB1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,19),(19,39),(39,59),(59,74),(74,77),(77,112),(112,412),(142,167),(167,169),(169,178),(178,183),(183,193),(193,300)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='B1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19])
        data.insert(loc=0,column='fileid', value=fileid)
        data[18]=current_date
        data[19]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEB1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')

        
    if (segment=='IEB2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,19),(39,59),(59,74),(19,39),(74,77),(183,193),(193,203),(77,112),(112,142),(142,167),(167,169),(169,178),(178,183)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='B2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19])
        data.insert(loc=0,column='fileid', value=fileid)
        data[14]=current_date
        data[15]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEB2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')  
        
        
    
    if (segment=='IEC1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,27),(27,31),(31,46),(46,56),(56,66),(66,76),(76,86),(86,88),(89,99),(99,144),(146,157),(186,197),(197,227),(227,239),(239,248),(248,300),(144,146),(157,175),(175,186)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='C1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])
        data.insert(loc=0,column='fileid', value=fileid)
        data[24]=current_date
        data[25]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEC1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')    
        
        
    if (segment=='IEC2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,25),(25,35),(35,45),(45,49),(49,51),(51,53),(53,56),(56,67),(68,71),(71,72),(72,74),(74,149),(149,224),(224,300)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='C2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20])
        data.insert(loc=0,column='fileid', value=fileid)
        data[19]=current_date
        data[20]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEC2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
    if (segment=='IEC3'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,35),(50,60),(35,50)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='C3']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9])
        data.insert(loc=0,column='fileid', value=fileid)
        data[8]=current_date
        data[9]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEC3 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
        
    if (segment=='IED1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,29)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='D1']
        data=data.filter(items=[0,2,3,4,5,6,7])
        data.insert(loc=0,column='fileid', value=fileid)
        data[6]=current_date
        data[7]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IED1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
            
            
        
    if (segment=='IEE1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,25),(25,29),(29,49),(49,69),(69,84),(84,86),(86,87),(87,97),(97,98),(98,99),(99,100),(109,129),(129,133),(133,135),(135,137),(159,169),(169,170),(170,171),(171,181),(181,300)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='E1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26])
        data.insert(loc=0,column='fileid', value=fileid)
        data[25]=current_date
        data[26]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEE1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
    if (segment=='IEG1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(48,58),(21,36),(38,48),(18,21),(36,38),(15,18)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='G1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12])
        data.insert(loc=0,column='fileid', value=fileid)
        data[11]=current_date
        data[12]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEG1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')     
        
    if (segment=='IEJ1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,16),(16,17),(17,37),(67,97),(97,127),(127,147),(37,67),(147,149),(149,158)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='J1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
        data.insert(loc=0,column='fileid', value=fileid)
        data[11]=current_date
        data[12]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEJ1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
    if (segment=='IEL1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,18),(18,20)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='L1']
        data=data.filter(items=[0,2,3,4,5,6,7,8])
        data.insert(loc=0,column='fileid', value=fileid)
        data[7]=current_date
        data[8]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEL1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')         
            
        
        
        
    if (segment=='IEH1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,18),(18,20),(20,22),(22,25),(25,300)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='H1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11])
        data.insert(loc=0,column='fileid', value=fileid)
        data[10]=current_date
        data[11]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEH1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
    if (segment=='IEH2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,45),(45,80),(80,110),(110,135),(135,137),(138,300)]

        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='H2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11])
        data.insert(loc=0,column='fileid', value=fileid)
        data[11]=current_date
        data[12]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEH2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')                                              
        
        
    if (segment=='IEI1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,19),(19,21),(21,26),(26,28),(28,31),(31,40),(40,46),(46,49),(49,56),(56,59),(59,68),(68,69),(69,70),(70,76),(76,78),(78,80),(80,82),(82,90),(90,115),(115,125),(125,129),(129,130),(130,170),(170,171),(171,172),(172,173),(173,174),(174,300)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='I1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35])
        data.insert(loc=0,column='fileid', value=fileid)
        data[34]=current_date
        data[35]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEI1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
    if (segment=='IEI2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,19),(19,22),(22,23),(23,28),(28,31),(31,32),(32,37),(37,42),(42,44),(44,47),(47,50),(50,51),(51,58),(58,69),(69,73),(73,77),(77,81),(81,82),(82,83),(83,84),(84,85),(85,86),(86,87),(87,88),(88,89),(89,90),(90,93),(93,104),(104,105),(106,107),(107,110),(110,111),(111,112),(112,114),(114,119),(119,300)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='I2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42])
        data.insert(loc=0,column='fileid', value=fileid)
        data[41]=current_date
        data[42]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEI2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')  
        
    if (segment=='IEI3'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,19),(19,39),(39,42),(42,45),(45,48),(48,54),(54,60),(60,70),(70,75),(75,79),(79,82),(82,85),(85,89),(89,93),(93,113),(113,119),(119,123),(123,126),(126,129),(129,133),(133,137),(137,157),(157,163),(163,167),(167,170),(170,173),(173,177),(177,181),(181,201),(201,207),(207,211),(211,214),(214,217),(217,221),(221,225),(225,245),(245,251),(251,255),(255,259),(299,303),(303,323),(323,329),(329,341)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='I3']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49])
        data.insert(loc=0,column='fileid', value=fileid)
        data[48]=current_date
        data[49]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEI3 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
    if (segment=='IEK1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,19),(19,30),(30,41),(41,50),(50,59),(59,65),(65,70),(70,83),(83,92),(92,101),(101,300)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='K1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19])
        data.insert(loc=0,column='fileid', value=fileid)
        data[18]=current_date
        data[19]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEK1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
    if (segment=='IEN1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(23,31),(20,23),(15,17),(17,20)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='N1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10])
        data.insert(loc=0,column='fileid', value=fileid)
        data[9]=current_date
        data[10]='IE'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('IEN1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')                             
              
        
 
def main(config, productconfig, env, input,segment):
    global save_main_session 
    global tablename
    global separatorchar 
    global remove_header_rows
    global remove_first_last_delim
    global gssourcebucket
    global tablefields
    global writedeposition
    global add_audit_fields
    global job_timestamp
    global jobname
    
    gssourcebucket = input
    ts = time.gmtime()
    job_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", ts)
  
    #Read environment configuration
    exec(compile(open(utilpath+'readconfig.py').read(), utilpath+'readconfig.py', 'exec'), globals())
    global env_config
    env_config = readconfig(config, env )
    #Read product configuration
    global product_config
    product_config = readconfig(productconfig, env)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
    process_ivans_file(input,segment) 

if __name__ == '__main__':
    """Input -> Config file, 
    product config file, 
    env, gcssourcefilename, Segment.
    """
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--config',
        required=True,
        help= ('Config file name with full path of file')
        )
    parser.add_argument(
        '--productconfig',
        required=True,
        help= ('Product config file name with full path of file')
        )
    parser.add_argument(
        '--env',
        required=True,
        help= ('Enviornment to be run dev/test/prod')
        )
    parser.add_argument(
        '--input',
        required=True,
        help= ('Blob (file) name with full path of bucket, gs://buckentname/folderifany/filename')
        )
    parser.add_argument(
        '--segment',
        required=True,
        help= ('Segment to extract the Segment data from file')
        )
    args = parser.parse_args()
    print((args.input))


    main(args.config,
         args.productconfig,
         args.env,
         args.input,
         args.segment
         ) 

