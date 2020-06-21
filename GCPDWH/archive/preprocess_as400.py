'''
Created on June 18, 2018
This module processes AS400 ivans fixed width files and 
generates one csv file per segment.
@author: Rajnikant Rakesh

'''



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


# current_date = datetime.datetime.now()
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
    blob.download_to_filename(remote_file_path+input)
    fileid=input[11:19]
    print(fileid)  
    
    if (segment=='A1'):    
        col_specification =[(1,7),(7,9),(9,12), (12,15),(15,20),(20,30),(30,40),(40,46),(46,56),(56,59)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None )
        data = data[data[1]=='A1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12])
        data.insert(loc=0,column='fileid', value=fileid)
        data[11]= current_date
        data[12]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('A1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
    if (segment=='B1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(43,47),(47,67),(67,87),(87,102),(102,105),(165,174),(174,194),(194,198),(198,229),(229,254),(254,256),(256,265),(270,275),(292,295),(295,298),(298,302),(308,311),(311,314),(314,318)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='B1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26])
        data.insert(loc=0,column='fileid', value=fileid)
        data[24]= current_date
        data[25]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('B1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
        
    if (segment=='B2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,27),(27,29),(29,33),(33,42),(42,43),(43,47),(47,67),(67,87),(87,102),(102,105),(105,135),(135,165),(165,174),(174,194),(194,199),(199,229),(229,254),(254,256),(256,265),(265,270),(270,275),(275,280),(280,283),(283,285),(285,288),(288,291),(291,292),(292,295),(295,298),(298,302),(302,307),(307,308),(308,311),(311,314),(314,318),(318,323),(323,326),(326,328),(328,330),(330,340),(340,350),(350,360),(360,370),(370,382),(382,412),(412,415)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='B2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52])
        data.insert(loc=0,column='fileid', value=fileid)
        data[51]= current_date
        data[52]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('B2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')    
        
    if (segment=='C1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(29,33),(33,42),(194,196),(196,197),(42,52),(52,62),(62,72),(72,82),(190,192),(205,216),(432,434),(434,445),(233,242),(15,27)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None , dtype={14:str})
        data = data[data[1]=='C1']
#         data[15]=data[15].astype(int64)
        mapping = [ ('}','0'),('J','1'),('K','2'),('L','3'),('M','4'),('N','5'),('O','6'),('P','7'),('Q','8'),('R','9') ]
        m= ['}','J','K','L','M','N','O','P','Q','R']
#         for k, v in mapping:
#             data[15] = data[15].str.replace(k, v)
            
        data[15]=data[15].where(data[15].str.find('}' or 'J'or 'K'or 'L'or 'M'or 'N'or 'O'or 'P'or 'Q'or 'R') < 0,data[15].str.replace('}','0').astype('int64',errors='ignore')*-1)
        data[15]=data[15].astype('int64',errors='ignore')/100
       #print data[15].str.find('}')
#         data[15]=data[15].astype(int64)
#         data[15]=data[15] * -1
#         data[15]=data[15] / 100
         
        print(data[15])
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20] )
        data.insert(loc=0,column='fileid', value=fileid)
        data[19]=current_date
        data[20]='AS400'
#         data=data.columns.trim
        
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('C1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
             
    if (segment=='C2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(52,62),(62,72),(72,82),(121,125),(126,128),(128,130),(147,150),(150,156),(156,165),(602,603),(608,610),(610,685),(685,800)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='C2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19])
        data.insert(loc=0,column='fileid', value=fileid)
        data[18]=current_date
        data[19]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('C2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
    if (segment=='C3'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,27),(27,29),(29,49),(49,64),(64,74),(74,83),(83,84),(84,104),(104,114),(114,117),(117,118),(118,133)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='C3']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18])
        data.insert(loc=0,column='fileid', value=fileid)
        data[19]=current_date
        data[20]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('C3 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
        
    if (segment=='D1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,17),(17,21),(21,30),(30,33),(33,47),(47,52),(52,62),(62,72),(72,82),(82,92),(92,93),(93,104),(104,115)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='D1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19])
        data.insert(loc=0,column='fileid', value=fileid)
        data[18]=current_date
        data[19]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('D1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')    
    
    if (segment=='G1'):    
        col_specification =[(1,7),(8,9),(10,12),(13,15),(16,17),(18,21),(22,30),(31,33),(34,36),(37,51),(52,52),(53,54),(55,94),(95,104),(105,114),(115,124),(125,135),(136,146),(147,149),(150,159),(160,160)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='G1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23])
        data.insert(loc=0,column='fileid', value=fileid)
        data[22]=current_date
        data[23]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('G1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')        
        
            
    if (segment=='I1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(32,36),(36,38),(38,43),(43,45),(45,47),(49,58),(58,64),(86,93),(94,99),(99,106),(106,107),(107,108),(110,112),(135,137),(137,139),(139,141),(146,154),(155,180),(181,191),(191,195),(196,197)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='I1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27])
        data.insert(loc=0,column='fileid', value=fileid)
        data[26]=current_date
        data[27]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('I1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
    if (segment=='E1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(42,45),(45,49),(49,69),(69,89),(89,104),(104,107),(107,108),(109,119),(119,120),(369,370),(121,122),(151,155),(131,151),(157,159),(181,191),(284,285),(287,289),(293,303),(367,368)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='E1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])
        data.insert(loc=0,column='fileid', value=fileid)
        data[24]=current_date
        data[25]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('E1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')
        
    if (segment=='H1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(30,33),(85,87),(145,147),(147,150)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None, dtype={5:str})
        data = data[data[1]=='H1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10])
        data.insert(loc=0,column='fileid', value=fileid)
        data[9]=current_date
        data[10]='AS400'
        data[5] = data[5].str.replace("*","")
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('H1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')  
        
    if (segment=='H2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(165,174),(174,194),(194,199),(199,229),(229,254),(254,256),(256,265)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='H2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10])
        data.insert(loc=0,column='fileid', value=fileid)
        data[12]=current_date
        data[13]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('H2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')    
        
    if (segment=='I2'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(32,36),(43,46),(46,47),(48,53),(53,56),(57,58),(58,61),(69,71),(76,79),(84,87),(91,92),(104,111),(120,131),(141,143),(147,149),(153,155),(178,179),(227,228),(228,229),(229,230),(230,231),(231,232),(232,233),(233,234),(234,235),(237,238),(242,253),(253,254),(254,255),(255,256),(259,262),(262,263),(263,264),(264,266),(266,271),(295,299)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='I2']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42])
        data.insert(loc=0,column='fileid', value=fileid)
        data[41]=current_date
        data[42]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('I2 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
    if (segment=='I3'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(32,36),(36,56),(56,59),(64,67),(67,70),(107,113),(113,119),(125,135),(135,139),(315,319),(319,323),(323,327),(327,347),(347,350),(350,356),(139,143),(143,146),(146,149),(149,153),(153,157),(157,177),(177,183),(183,187),(187,190),(190,193),(193,197),(197,201),(201,221),(221,227),(227,231),(231,234),(234,237),(237,241),(241,245),(245,265),(265,271),(271,275),(275,278),(278,281),(281,285),(285,289),(289,309),(309,315)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='I3']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49])
        data.insert(loc=0,column='fileid', value=fileid)
        data[48]=current_date
        data[49]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('I3 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')   
        
    if (segment=='K1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(32,35),(35,46),(62,73),(73,82),(82,91),(108,114),(135,140),(142,155),(155,164),(164,173)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='K1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16])
        data.insert(loc=0,column='fileid', value=fileid)
        data[15]=current_date
        data[16]='AS400'
        mapping = [ ('}','0'),('J','1'),('K','2'),('L','3'),('M','4'),('N','5'),('O','6'),('P','7'),('Q','8'),('R','9') ]
        for k, v in mapping:
            data[5] = data[5].str.replace(k, v)
        data[5]=data[5].astype(int64)        
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('K1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')      
        
    if (segment=='J1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,17),(17,20),(20,23),(23,30),(30,31),(31,37),(37,43),(43,44),(44,64),(64,68),(68,77),(77,87),(87,97),(97,108),(108,119),(119,122),(122,152),(152,182),(182,212),(212,232),(232,234),(234,243),(243,246),(246,256),(256,263),(263,273),(273,283),(283,285),(285,295),(295,305),(305,307),(307,407)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='J1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38])
        data.insert(loc=0,column='fileid', value=fileid)
        data[37]=current_date
        data[38]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('J1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...') 
        
    if (segment=='L1'):    
        col_specification =[(1,7),(8,9),(10,12),(13,15),(16,26),(27,29),(30,32),(33,35),(36,37),(38,38),(39,43),(44,50),(51,53),(54,54),(55,56)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='J1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17])
        data.insert(loc=0,column='fileid', value=fileid)
        data[16]=current_date
        data[17]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('J1 Segement File processed...')
        Blob= bucket.blob(outputfilename)
        Blob.upload_from_filename(remote_file_path+outputfilename)
        logging.info('Processed File uploaded to GCS bucket...')                            
                   
                   
    if (segment=='M1'):    
        col_specification =[(1,7),(7,9),(9,12),(12,15),(15,17),(17,21),(21,30),(30,33),(33,36),(36,42),(42,45),(45,55),(55,66),(66,76),(76,87),(87,96),(96,146),(146,152),(152,177),(177,182),(182,184),(184,189),(189,192),(192,195),(195,197),(197,199),(199,204),(204,209),(209,218),(218,221),(221,223),(223,225),(225,227),(227,231),(231,233),(233,235),(235,237),(237,240),(240,242),(242,292),(292,303),(303,314),(314,318),(318,328),(328,338),(338,348)]
        data = pd.read_fwf(remote_file_path+input, colspecs=col_specification,header=None)
        data = data[data[1]=='M1']
        data=data.filter(items=[0,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38])
        data.insert(loc=0,column='fileid', value=fileid)
        data[47]=current_date
        data[48]='AS400'
        outputfilename=segment+"_"+input.replace('.DAT','.csv')
        data.to_csv(remote_file_path+outputfilename,sep ='|',header =None,index=False)
        logging.info('J1 Segement File processed...')
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

