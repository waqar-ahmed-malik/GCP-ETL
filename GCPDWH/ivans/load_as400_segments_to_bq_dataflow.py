'''

Created on March 30, 2018

This module loads a Bigquery Table from Google Cloud Storage bucket.

Module needs bucket name to be read and destination Bigquery Table name.

It picks up the environment and product information from respective config files

job name for pipeline is generated based on dataset name and table name

@author: Rajnikant Rakesh

Arguments Reuired : 

Example - 

--config "config.properties" --productconfig "ivans.properties" --env "dev"   --separator "|"  --stripheader "0" --stripdelim "0"  --addaudit "1"  --writeDeposition "WRITE_APPEND"  --system "AS400" --input "gs://dw-dev-insurance/ivans/archive/AS400_NCNU_20180227.DAT"



'''





import apache_beam as beam

from apache_beam import pvalue

from apache_beam import pvalue

import argparse

import logging

import os

import sys

from google.cloud import bigquery

from googleapiclient import discovery

from oauth2client.client import GoogleCredentials

import time

import httplib2

from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd

import datetime

from pandas._libs.index import date

from numpy import dtype, int64

from builtins import int



cwd = os.path.dirname(os.path.abspath(__file__))



if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    

def readschema():

    for seg in segments:

        if srcsystem =='AS400' and seg !='N1' :

            tblname = srcsystem+"_"+seg+"_LDG"

            schema = readtableschema(env_config['projectid'], product_config['datasetname'], tblname)

            tablefields[seg]=schema

        elif srcsystem =='IE': 

            print("not a AS400 file")

    

class filter_segments(beam.DoFn):

    def process(self, element, segment):

        """This Function will Filter data based on segments passed""" 

        if element.find(segment,7,9)>0:

           yield element

        else:

            return 

        

def processsegments(inputdata, seg):

    filedate=gssourcebucket.split('/')[4].split('_')[2].split('.')[0]

    print("----------------", filedate)

    if seg =='A1':

       col_specification =[0,7,9,12,15,20,30,40,46,56,59]

       row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

       row.pop()

       del row[1]   

       row.insert(0,filedate )

       logging.info('Processed Row is ... %s', row ) 

       record='|'.join(row)

       logging.info('Processed record is ... %s', record )         

       return  record

    elif seg == 'B1':

         col_specification =[0,7,9,12,15,43,47,67,87,102,105,165,174,194,198,229,254,256,265,270,275,292,295,298,302,308,311,314,318]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(9)

         row.pop(16)

         row.pop(17)

         row.pop(20)

         row.pop()

         del row[1]   

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

    

    elif seg == 'B2':

         col_specification =[0,7,9,12,15,27,29,33,42,43,47,67,87,102,105,135,165,174,194,199,229,254,256,265,270,275,280,283,285,288,291,292,295,298,302,307,308,311,314,318,323,326,328,330,340,350,360,370,382,412,415,425]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]   

         row.insert(0,filedate )

         if row[28].find('}')>0:

               row[28] = row[28].replace('}','0')

         else:

           row[28]

         if row[28].find('Q')>0:

               row[28] = row[28].replace('Q','8')

         else:

           row[28]

         if row[28].find('H')>0:

               row[28] = row[28].replace('H','0')

         else:

           row[28]

         if row[28].find('l')>0:

               row[28] = row[28].replace('l','0')

         else:

           row[28]

         if row[28].find('J')>0:

               row[28] = row[28].replace('J','0')

         else:

           row[28]

         if row[28].find('K')>0:

               row[28] = row[28].replace('K','0')

         else:

           row[28] 

         if row[28].find('L')>0:

               row[28] = row[28].replace('L','0')

         else:

           row[28]

         if row[28].find('M')>0:

               row[28] = row[28].replace('M','0')

         else:

           row[28]

         if row[28].find('N')>0:

               row[28] = row[28].replace('N','0')

         else:

           row[28]

         if row[28].find('O')>0:

               row[28] = row[28].replace('O','0')

         else:

           row[28]

         if row[28].find('P')>0:

               row[28] = row[28].replace('P','0')

         else:

           row[28]

 

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

       

    elif seg == 'C1':

         col_specification =[0,7,9,12,15,27,29,33,42,52,62,72,82,190,192,194,196,197,205,216,233,242,432,434,444]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(5)

         row.pop(11)

         row.pop(12)

         row.pop(14)

         row.pop(15)

         row.pop(16)

         row.pop()

          

         if row[14].find('}')>0:

                 row[14] = row[14].replace('}','0')

                 row[14] = int(row[14])*-1

                 row[14] = row[14]/100

                 row[14] = str(row[14])

         else:

             row[14]= int(row[14])/100

             row[14] = str(row[14])  

              

         if row[17].find('}')>0:

                 row[17] = row[17].replace('}','0')

                 row[17] = int(row[17])*-1

                 row[17] = row[17]/100

                 row[17] = str(row[17])

         else:

             row[17]= int(row[17])/100 

             row[17] = str(row[17])    

 

         del row[1]

         row.insert(0,filedate)

          

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

    elif seg == 'C2':

         col_specification =[0,7,9,12,15,52,62,72,82,121,125,126,128,130,147,150,156,165,602,603,608,610,685,800]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(7)

         row.pop(8)

         row.pop(10)

         row.pop(13)

         row.pop(14)

         row.pop()

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

      

    elif seg=='C3':    

         col_specification =[0,7,9,12,15,27,28,49,64,74,83,84,104,114,117,118,133]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

    elif seg == 'D1':

         col_specification =[0,7,9,12,15,17,21,30,33,47,52,62,72,82,92,93,104,115]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         

         if row[9].find('Q')>0:

            row[9] = row[9].replace('Q','8')

         else:

            row[9]

  

         if row[9].find('}')>0:

                row[9] = row[9].replace('}','0')

         else:

            row[9] 

 

         if row[9].find('J')>0:

                row[9] = row[9].replace('J','1')

         else:

            row[9] 

     

         if row[9].find('K')>0:

                row[9] = row[9].replace('K','2')

         else:

            row[9] 

 

         if row[9].find('L')>0:

                row[9] = row[9].replace('L','3')

         else:

            row[9]

     

         if row[9].find('M')>0:

                row[9] = row[9].replace('M','4')

         else:

            row[9] 

 

         if row[9].find('N')>0:

                row[9] = row[9].replace('N','5')

         else:

            row[9]  

 

         if row[9].find('O')>0:

                row[9] = row[9].replace('O','6')

         else:

            row[9]  

 

         if row[9].find('P')>0:

                row[9] = row[9].replace('P','7')

         else:

            row[9] 

 

         if row[9].find('Q')>0:

                row[9] = row[9].replace('Q','8')

         else:

            row[9]

     

         if row[9].find('R')>0:

                row[9] = row[9].replace('R','9')

         else:

            row[9]

 

 

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

     

      

    elif seg == 'E1':

         col_specification = [0,7,9,12,15,42,45,49,69,89,104,107,108,109,119,120,121,122,131,151,155,157,159,181,191,284,285,287,289,293,303,367,368,369,370]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(11)

         row.pop(13)

         row.pop(14)

         row.pop(16)

         row.pop(17)

         row.pop(18)

         row.pop(19)

         row.pop(20)

         row.pop(21)

         row.pop(22)

         row.pop()

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

      

    elif seg == 'G1':

         col_specification =[0,7,9,12,15,17,21,30,33,36,51,52,54,94,104,114,124,135,146,149,159,160]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

     

    elif seg == 'H1':

         col_specification =[0,7,9,12,15,30,33,85,87,145,147,150] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(5)

         row.pop(6)

         row.pop()

          

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record

     

    elif seg == 'H2':

         col_specification =[0,7,9,12,15,165,174,194,199,228,254,256,265]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4) 

         row.pop()

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record 

      

    elif seg == 'I1':

         col_specification =[0,7,9,12,15,32,36,38,43,45,47,49,58,64,86,93,94,99,106,107,108,110,112,135,137,139,141,146,154,155,180,181,191,195,196,197]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(9)

         row.pop(11)

         row.pop(12)

         row.pop(16)

         row.pop(17)

         row.pop(20)

         row.pop(21)

         row.pop(22)

         row.pop(24)

         row.pop()

         del row[1]

         row.insert(0,filedate )

 

         if row[6].find('Q')>0:

                row[6] = row[6].replace('Q','8')

         else:

            row[6] 

  

         if row[6].find('}')>0:

                row[6] = row[6].replace('}','0')

         else:

            row[6]  

 

         if row[6].find('J')>0:

                row[6] = row[6].replace('J','1')

         else:

            row[6] 

     

         if row[6].find('K')>0:

                row[6] = row[6].replace('K','2')

         else:

            row[6] 

 

         if row[6].find('L')>0:

                row[6] = row[6].replace('L','3')

         else:

            row[6]

     

         if row[6].find('M')>0:

                row[6] = row[6].replace('M','4')

         else:

            row[6] 

 

         if row[6].find('N')>0:

                row[6] = row[6].replace('N','5')

         else:

            row[6]

 

         if row[6].find('O')>0:

                row[6] = row[6].replace('O','6')

         else:

            row[6] 

 

         if row[6].find('P')>0:

                row[6] = row[6].replace('P','7')

         else:

            row[6] 

 

         if row[6].find('Q')>0:

                row[6] = row[6].replace('Q','8')

         else:

            row[6]

     

         if row[6].find('R')>0:

                row[6] = row[6].replace('R','9')

         else:

            row[6]

 

         if row[6].find('H')>0:

                row[6] = row[6].replace('H','0')

         else:

            row[6]    

 

 

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record  

    elif seg == 'I2':

         col_specification =[0,7,9,12,15,32,36,43,46,47,48,53,56,57,58,61,69,71,76,79,84,87,91,92,104,111,120,131,141,143,147,149,153,155,178,179,227,228,229,230,231,232,233,234,235,237,238,242,253,254,255,256,259,262,263,264,266,271,295,299]     

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(5)

         row.pop(7)

         row.pop(9)

         row.pop(11)

         row.pop(12)

         row.pop(13)

         row.pop(14)

         row.pop(15)

         row.pop(16)

         row.pop(17)

         row.pop(18)

         row.pop(19)

         row.pop(20)

         row.pop(21)

         row.pop(29)

         row.pop(30)

         row.pop(34)

         row.pop(39)

         row.pop()

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record    

      

    elif seg == 'I3':

         col_specification =[0,7,9,12,15,32,36,56,59,64,67,70,107,113,119,125,135,139,143,146,149,153,157,177,183,187,190,193,197,201,221,227,231,234,237,241,245,265,271,275,278,281,285,289,309,315,319,63,67,347,350,356]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(7)

         row.pop(9)

         row.pop(11)

         row.pop()

         #row.pop(43)

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record 

     

    elif seg == 'J1':

         col_specification =[0,7,9,12,15,17,20,23,30,31,37,43,44,64,68,77,87,97,108,119,122,152,182,212,26,234,243,246,256,263,273,283,285,295,305,307,407] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

          

         if row[31].find('}')>0:

                row[31] = row[31].replace('}','0')

         else:

            row[31]

 

         if row[32].find('Q')>0:

                row[32] = row[32].replace('Q','8')

         else:

            row[32]

  

         if row[32].find('}')>0:

                row[32] = row[32].replace('}','0')

         else:

            row[32] 

 

         if row[32].find('J')>0:

                row[32] = row[32].replace('J','1')

         else:

            row[32]

     

         if row[32].find('K')>0:

                row[32] = row[32].replace('K','2')

         else:

            row[32]

            

         if row[32].find('L')>0:

                row[32] = row[32].replace('L','3')

         else:

            row[32] 

     

         if row[32].find('M')>0:

                row[32] = row[32].replace('M','4')

         else:

            row[32]  

 

         if row[32].find('N')>0:

                row[32] = row[32].replace('N','5')

         else:

            row[32]  

 

         if row[32].find('O')>0:

                row[32] = row[32].replace('O','6')

         else:

            row[32] 

 

         if row[32].find('P')>0:

                row[32] = row[32].replace('P','7')

         else:

            row[32] 

 

         if row[32].find('Q')>0:

                row[32] = row[32].replace('Q','8')

         else:

            row[32]

     

         if row[32].find('R')>0:

                row[32] = row[32].replace('R','9')

         else:

            row[32]

 

         if row[33].find('H')>0:

                row[33] = row[33].replace('H','0')

         else:

            row[33] 

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

      

    elif seg == 'K1':

         col_specification =[0,7,9,12,15,32,35,45,62,73,82,91,108,114,135,140,142,155,164,173]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(4)

         row.pop(6)

         row.pop(9)

         row.pop(10)

         row.pop(11)

         row.pop()

         del row[1]

         row.insert(0,filedate )

         if row[4].find('}')>0:

                row[4] = row[4].replace('}','0')

                

         else:

            row[4]

              

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

      

    elif seg == 'L1':

         col_specification =[0,7,9,12,15,26,28,32,35,37,38,43,50,53,54,56]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         if row[11].find('}')>0:

                 row[11] = row[11].replace('}','0')

         else:

             row[11]               

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

      

    elif seg == 'M1':

         col_specification =[0,7,9,12,15,17,21,30,33,36,42,45,55,66,76,87,96,146,152,177,182,184,189,192,195,197,199,204,209,218,221,223,225,227,231,233,235,237,240,242,282,303,314,318,68,338,348]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

 

     

    else :

        return pvalue.TaggedOutput('error_rows', inputdata)

         

def rowextractor(validrow,seg):

    """reads input and splits to relevant row format

    1.strip control characters and quotes in the row

    2.strip leading and trailing whitespace in the row

    3.Conditionally pop first and last char only if an input flag is specified

    4.split the row into individual column records and cleanse individual columns further

    5.if adding audit fields, add 2 elements to the input list, the current timestamp and Jobname

    6.strip leading and trailing whitespace per field

    7.strip content from fields containing only strings '(null)' or '()' - this can be modified further

    """

    

    logging.info("Starting row processing in row extractor")

    try:

        rec = {}

        i=0

        validrow = validrow.replace('\r', ' ').replace('\n', ' ').replace('\0',' ').replace('"',' ')

        validrow = validrow.strip()

        if(remove_first_last_delim):

            tmp_row = validrow[1:-1]

        else:

           tmp_row = validrow

        

        row = tmp_row.split(separatorchar)

        if(add_audit_fields):

            row.extend([job_timestamp,srcsystem])

            

        for field in tablefields[seg]:

            if row[i]:

                rec[field] = row[i].strip()

                if ((rec[field]== '(null)') or  (rec[field] == '()')):

                    rec[field] = '' 

            else:

                rec[field] = None

            i=i+1

    except:

        logging.error("Error in processing row")

        raise

    return rec



def error_row_handler (error_row):

    logging.info("Entered error handler, %s",error_row)



def run():

    """

    1. Set Dataflow PipeLine configurations.

    2. Create PCollection element for each line read from the delimited file.

    3. Tag values by calling RowValidator method i.e. clean records or broken records.

    4. Call rowextractor method for cleaned records.

    5. Write valid/clean records to BigQuery table mentioned in the parameter.

    6. Sink the error records to error handler.  

    """

    

    pipeline_args = ['--project', env_config['projectid'],

                     '--job_name', jobname,

                     '--runner', env_config['runner'],

                     '--staging_location', product_config['stagingbucket'],

                     '--temp_location', product_config['tempbucket'],

#                     '--requirements_file', env_config['requirements_file'],

                     '--save_main_session', 'True',

                     '--region', env_config['region'],

                    '--zone',env_config['zone'],

                    '--network',env_config['network'],

                    '--subnetwork',env_config['subnetwork'],

                     '--num_workers', env_config['num_workers'],

                     '--max_num_workers', env_config['max_num_workers'],

                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],

                     '--service_account_name', env_config['service_account_name'],

                     '--service_account_key_file', env_config['service_account_key_file']

                     ]

    

    try:



        pcoll = beam.Pipeline(argv=pipeline_args)

        readlines = pcoll  | 'Read Records' >> beam.io.ReadFromText(gssourcebucket,skip_header_lines = remove_header_rows)

        

        A1 = (readlines | 'Validate Segment A1' >> beam.ParDo(filter_segments(),segment='A1'))

        B1 = (readlines | 'Validate Segment B1' >> beam.ParDo(filter_segments(),segment='B1'))

        B2 = (readlines | 'Validate Segment B2' >> beam.ParDo(filter_segments(),segment='B2'))

        C1 = (readlines | 'Validate Segment C1' >> beam.ParDo(filter_segments(),segment='C1'))

        C2 = (readlines | 'Validate Segment C2' >> beam.ParDo(filter_segments(),segment='C2'))

        C3 = (readlines | 'Validate Segment C3' >> beam.ParDo(filter_segments(),segment='C3'))

        D1 = (readlines | 'Validate Segment D1' >> beam.ParDo(filter_segments(),segment='D1'))

        E1 = (readlines | 'Validate Segment E1' >> beam.ParDo(filter_segments(),segment='E1'))

        G1 = (readlines | 'Validate Segment G1' >> beam.ParDo(filter_segments(),segment='G1'))

        H1 = (readlines | 'Validate Segment H1' >> beam.ParDo(filter_segments(),segment='H1'))

        H2 = (readlines | 'Validate Segment H2' >> beam.ParDo(filter_segments(),segment='H2'))

        I1 = (readlines | 'Validate Segment I1' >> beam.ParDo(filter_segments(),segment='I1'))

        I2 = (readlines | 'Validate Segment I2' >> beam.ParDo(filter_segments(),segment='I2'))

        I3 = (readlines | 'Validate Segment I3' >> beam.ParDo(filter_segments(),segment='I3'))

        J1 = (readlines | 'Validate Segment J1' >> beam.ParDo(filter_segments(),segment='J1'))

        K1 = (readlines | 'Validate Segment K1' >> beam.ParDo(filter_segments(),segment='K1'))

        L1 = (readlines | 'Validate Segment L1' >> beam.ParDo(filter_segments(),segment='L1'))

        M1 = (readlines | 'Validate Segment M1' >> beam.ParDo(filter_segments(),segment='M1'))

        

       

        a1_valid_rows, error_rows = (A1 | 'Process Segment A1' >> beam.Map(processsegments,seg='A1').with_outputs('error_rows',main='valid_rows'))

        b1_valid_rows, error_rows = (B1 | 'Process Segments B1' >> beam.Map(processsegments,seg='B1').with_outputs('error_rows',main='valid_rows'))

        b2_valid_rows, error_rows = (B2 | 'Process Segments B2' >> beam.Map(processsegments,seg='B2').with_outputs('error_rows',main='valid_rows'))

        c1_valid_rows, error_rows = (C1 | 'Process Segments C1' >> beam.Map(processsegments,seg='C1').with_outputs('error_rows',main='valid_rows'))

        c2_valid_rows, error_rows = (C2 | 'Process Segments C2' >> beam.Map(processsegments,seg='C2').with_outputs('error_rows',main='valid_rows'))

        c3_valid_rows, error_rows = (C3 | 'Process Segments C3' >> beam.Map(processsegments,seg='C3').with_outputs('error_rows',main='valid_rows'))

        d1_valid_rows, error_rows = (D1 | 'Process Segments D1' >> beam.Map(processsegments,seg='D1').with_outputs('error_rows',main='valid_rows'))

        e1_valid_rows, error_rows = (E1 | 'Process Segments E1' >> beam.Map(processsegments,seg='E1').with_outputs('error_rows',main='valid_rows'))

        g1_valid_rows, error_rows = (G1 | 'Process Segments G1' >> beam.Map(processsegments,seg='G1').with_outputs('error_rows',main='valid_rows'))

        h1_valid_rows, error_rows = (H1 | 'Process Segments H1' >> beam.Map(processsegments,seg='H1').with_outputs('error_rows',main='valid_rows'))

        h2_valid_rows, error_rows = (H2 | 'Process Segments H2' >> beam.Map(processsegments,seg='H2').with_outputs('error_rows',main='valid_rows'))

        i1_valid_rows, error_rows = (I1 | 'Process Segments I1' >> beam.Map(processsegments,seg='I1').with_outputs('error_rows',main='valid_rows'))

        i2_valid_rows, error_rows = (I2 | 'Process Segments I2' >> beam.Map(processsegments,seg='I2').with_outputs('error_rows',main='valid_rows'))

        i3_valid_rows, error_rows = (I3 | 'Process Segments I3' >> beam.Map(processsegments,seg='I3').with_outputs('error_rows',main='valid_rows'))

        

        j1_valid_rows, error_rows = (J1 | 'Process Segments J1' >> beam.Map(processsegments,seg='J1').with_outputs('error_rows',main='valid_rows'))

        k1_valid_rows, error_rows = (K1 | 'Process Segments K1' >> beam.Map(processsegments,seg='K1').with_outputs('error_rows',main='valid_rows'))

        l1_valid_rows, error_rows = (L1 | 'Process Segments L1' >> beam.Map(processsegments,seg='L1').with_outputs('error_rows',main='valid_rows'))

        m1_valid_rows, error_rows = (M1 | 'Process Segments M1' >> beam.Map(processsegments,seg='M1').with_outputs('error_rows',main='valid_rows'))

           

       

        a1_clean_rows, a1_error_records = a1_valid_rows | 'Cleanse Segment A1' >> beam.Map(rowextractor,seg='A1').with_outputs('error_records',main='valid_rows')

        b1_clean_rows, b1_error_records = b1_valid_rows | 'Cleanse Segment B1' >> beam.Map(rowextractor,seg='B1').with_outputs('error_records',main='valid_rows')

        b2_clean_rows, b2_error_records = b2_valid_rows | 'Cleanse Segment B2' >> beam.Map(rowextractor,seg='B2').with_outputs('error_records',main='valid_rows')

        c1_clean_rows, c1_error_records = c1_valid_rows | 'Cleanse Segment C1' >> beam.Map(rowextractor,seg='C1').with_outputs('error_records',main='valid_rows')

        c2_clean_rows, c2_error_records = c2_valid_rows | 'Cleanse Segment C2' >> beam.Map(rowextractor,seg='C2').with_outputs('error_records',main='valid_rows')

        c3_clean_rows, c3_error_records = c3_valid_rows | 'Cleanse Segment C3' >> beam.Map(rowextractor,seg='C3').with_outputs('error_records',main='valid_rows')

        d1_clean_rows, d1_error_records = d1_valid_rows | 'Cleanse Segment D1' >> beam.Map(rowextractor,seg='D1').with_outputs('error_records',main='valid_rows')

        e1_clean_rows, e1_error_records = e1_valid_rows | 'Cleanse Segment E1' >> beam.Map(rowextractor,seg='E1').with_outputs('error_records',main='valid_rows')

        g1_clean_rows, g1_error_records = g1_valid_rows | 'Cleanse Segment G1' >> beam.Map(rowextractor,seg='G1').with_outputs('error_records',main='valid_rows')

        h1_clean_rows, h1_error_records = h1_valid_rows | 'Cleanse Segment H1' >> beam.Map(rowextractor,seg='H1').with_outputs('error_records',main='valid_rows')        

        h2_clean_rows, h2_error_records = h2_valid_rows | 'Cleanse Segment H2' >> beam.Map(rowextractor,seg='H2').with_outputs('error_records',main='valid_rows')

        i1_clean_rows, i1_error_records = i1_valid_rows | 'Cleanse Segment I1' >> beam.Map(rowextractor,seg='I1').with_outputs('error_records',main='valid_rows')

        i2_clean_rows, i2_error_records = i2_valid_rows | 'Cleanse Segment I2' >> beam.Map(rowextractor,seg='I2').with_outputs('error_records',main='valid_rows')  

        i3_clean_rows, i3_error_records = i3_valid_rows | 'Cleanse Segment I3' >> beam.Map(rowextractor,seg='I3').with_outputs('error_records',main='valid_rows')      

        j1_clean_rows, j1_error_records = j1_valid_rows | 'Cleanse Segment J1' >> beam.Map(rowextractor,seg='J1').with_outputs('error_records',main='valid_rows')

        k1_clean_rows, k1_error_records = k1_valid_rows | 'Cleanse Segment K1' >> beam.Map(rowextractor,seg='K1').with_outputs('error_records',main='valid_rows')

        l1_clean_rows, l1_error_records = l1_valid_rows | 'Cleanse Segment L1' >> beam.Map(rowextractor,seg='L1').with_outputs('error_records',main='valid_rows')

        m1_clean_rows, m1_error_records = m1_valid_rows | 'Cleanse Segment M1' >> beam.Map(rowextractor,seg='M1').with_outputs('error_records',main='valid_rows')

        

       

        

        a1_clean_rows | 'WriteBQ Segment A1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                 product_config['datasetname']+"."+"AS400_A1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                 ,write_disposition=writedeposition

                  ))

        b1_clean_rows | 'WriteBQ Segment B1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                 product_config['datasetname']+"."+"AS400_B1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                 ,write_disposition=writedeposition

                  ))

          

        b2_clean_rows | 'WriteBQ Segment B2'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                 product_config['datasetname']+"."+"AS400_B2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                 ,write_disposition=writedeposition

                 ))

          

        c1_clean_rows | 'WriteBQ Segment C1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_C1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

           

        c2_clean_rows | 'WriteBQ Segment C2'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_C2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

          

        c3_clean_rows | 'WriteBQ Segment C3'    >> beam.io.Write(

               beam.io.BigQuerySink(

                   product_config['datasetname']+"."+"AS400_C3_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

          

        d1_clean_rows | 'WriteBQ Segment D1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_D1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

           

        e1_clean_rows | 'WriteBQ Segment E1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"AS400_E1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                   ))

           

        g1_clean_rows | 'WriteBQ Segment G1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_G1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

           

        h1_clean_rows | 'WriteBQ Segment H1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_H1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

           

        h2_clean_rows | 'WriteBQ Segment H2'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_H2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

           

        i1_clean_rows | 'WriteBQ Segment I1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"AS400_I1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        i2_clean_rows | 'WriteBQ Segment I2'    >> beam.io.Write(

               beam.io.BigQuerySink(

                  product_config['datasetname']+"."+"AS400_I2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                  ,write_disposition=writedeposition

                 ))

          

        i3_clean_rows | 'WriteBQ Segment I3'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_I3_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                    ))

        

        j1_clean_rows | 'WriteBQ Segment J1'    >> beam.io.Write(

               beam.io.BigQuerySink(

                  product_config['datasetname']+"."+"AS400_J1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                  ,write_disposition=writedeposition

                 ))

#           

        k1_clean_rows | 'WriteBQ Segment K1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_K1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

            

        l1_clean_rows | 'WriteBQ Segment L1'    >> beam.io.Write(

                  beam.io.BigQuerySink(

                      product_config['datasetname']+"."+"AS400_L1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                      ,write_disposition=writedeposition

                      ))

            

        m1_clean_rows | 'WriteBQ Segment M1'    >> beam.io.Write(

                 beam.io.BigQuerySink(

                     product_config['datasetname']+"."+"AS400_M1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                     ,write_disposition=writedeposition

                     ))

         

        a1_error_records | 'Handler-ErrorRow A1'>> beam.Map(error_row_handler)     

        b1_error_records | 'Handler-ErrorRow B1'>> beam.Map(error_row_handler) 

        b2_error_records | 'Handler-ErrorRow B2'>> beam.Map(error_row_handler) 

        c1_error_records | 'Handler-ErrorRow C1'>> beam.Map(error_row_handler) 

        c2_error_records | 'Handler-ErrorRow C2'>> beam.Map(error_row_handler) 

        c3_error_records | 'Handler-ErrorRow C3'>> beam.Map(error_row_handler) 

        d1_error_records | 'Handler-ErrorRow D1'>> beam.Map(error_row_handler) 

        e1_error_records | 'Handler-ErrorRow E1'>> beam.Map(error_row_handler) 

        g1_error_records | 'Handler-ErrorRow G1'>> beam.Map(error_row_handler) 

        h1_error_records | 'Handler-ErrorRow H1'>> beam.Map(error_row_handler) 

        h2_error_records | 'Handler-ErrorRow H2'>> beam.Map(error_row_handler) 

        i1_error_records | 'Handler-ErrorRow I1'>> beam.Map(error_row_handler) 

        i2_error_records | 'Handler-ErrorRow I2'>> beam.Map(error_row_handler) 

        i3_error_records | 'Handler-ErrorRow I3'>> beam.Map(error_row_handler) 

        j1_error_records | 'Handler-ErrorRow J1'>> beam.Map(error_row_handler)   

        k1_error_records | 'Handler-ErrorRow K1'>> beam.Map(error_row_handler) 

        l1_error_records | 'Handler-ErrorRow L1'>> beam.Map(error_row_handler)  

        m1_error_records | 'Handler-ErrorRow M1'>> beam.Map(error_row_handler)

        



        p = pcoll.run()

        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise

    

def main(config, productconfig, env, separator, stripheader, stripdelim, addaudit,system,input, writeDeposition='WRITE_TRUNCATE'):

    logging.getLogger().setLevel(logging.INFO)

    """

    1. Define required global variables. 

    2. Set Deposition method to WRITE_TRUNCATE in case not specified tough it is required parameter.

    3. Generate stream for Audit fields in data. i.e. BQ_CREATED_BY= {DataFlow Job Name} and BQ_CREATE_TS= {Dataflow Job Start Time}

    4. Read config.properties by calling readconfig method from readconfig.py file

    5. Authenticate GCP project, read json file from config file. (This is explict authentication)

    6. Read BigQuery Table schema using readshcema method from readschema.py file.

    7. Call DataFlow PipeLine method, this initiate Dataflow job on GCP.

    8. Call wait_for_dataflow_job_finish method to monitor the load till it finishes.

    """

    global save_main_session 

    global separatorchar 

    global remove_header_rows

    global remove_first_last_delim

    global gssourcebucket

    global tablefields

    global writedeposition

    global add_audit_fields

    global job_timestamp

    global jobname

    global projectid

    global srcsystem

    global segments

    global filedate

    

    segments = ['A1','B1','B2','C1','C2','C3','D1','E1','G1','H1','H2','I1','I2','I3','J1','K1','L1','M1','N1']           

    if writeDeposition=='WRITE_TRUNCATE':

        writedeposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

    else:

        writedeposition=beam.io.BigQueryDisposition.WRITE_APPEND

    

    tablefields = {}

    save_main_session = True

    separatorchar = separator

    remove_header_rows = stripheader

    remove_first_last_delim = stripdelim

    add_audit_fields=addaudit

    gssourcebucket = input

    filedate=gssourcebucket.split('/')[4].split('_')[2].split('.')[0]

    ts = time.gmtime()

    job_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", ts)

    time.strftime("%Y%m%d")

    srcsystem=system



    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())

    global env_config

    env_config = readconfig(config, env )

    

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']

  

    global product_config

    product_config = readconfig(productconfig, env)

    projectid=env_config['projectid']

    exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals())

    jobname = "load-" + product_config['productname']+"-"+ srcsystem+"-segment-"+product_config['datasetname'] +"-"+filedate

    print(jobname)

    readschema()

    run()

    

    ''' Move files to Archive folder '''

    logging.info("Archiving files...")

    movefiles= "gsutil mv gs://"+product_config['bucket']+"/current/AS400_NCNU_*"+" gs://"+product_config['bucket']+"/archive/"

    print(movefiles)

    os.system(movefiles)

    logging.info("Files has been successfully copied from current folder to archive folder..") 

    sys.exit()

        

if __name__ == '__main__':

    """Input -> Config file, 

    product config file, 

    env, gcssourcefilename, tablename.

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

        required=False,

        help= ('Blob (file) name with full path of bucket, gs://buckentname/folderifany/filename')

        )

    parser.add_argument(

        '--separator',

        required=True,

        help= ('Separator character used in source file (gcs file)')

        )

    parser.add_argument(

        '--stripheader',

        type = int,

        required=True,

        help= ('0|1 Strip header row from input file')

        )

    parser.add_argument(

        '--stripdelim',

        type=int,

        required=True,

        help= ('0|1 Strip first and last delimiter characters from each row of the input file')

        )

    parser.add_argument(

        '--addaudit',

        type = int,

        required=True,

        help= ('0|1 Add timestamp and jobowner fields to each input line')

        )

 

    parser.add_argument(

        '--output',

        required=False,

        help= ('Target BigQuery table for loading data specified as tablename')

      )

    parser.add_argument(

        '--writeDeposition',

        required=False,

        default='WRITE_APPEND',

        help= ('WRITE_APPEND by Default, WRITE_TRUNCATE to truncate')

      )

    parser.add_argument(

        '--system',

        required=True,

        default='',

        help= ('string, if any, to append to the end of the dataflow jobname, such as a batch_id or timestamp')

      )

  

    args = parser.parse_args()

    print((args.input))





    main(args.config,

         args.productconfig,

         args.env,

         args.separator,

         args.stripheader,

         args.stripdelim,

         args.addaudit,

         args.system,

         args.input,

         args.writeDeposition         

         )

                
