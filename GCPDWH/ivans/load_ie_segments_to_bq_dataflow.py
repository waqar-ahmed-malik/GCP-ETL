'''

Created on July 11, 2018

This module loads a Bigquery Table from Google Cloud Storage bucket.

Module needs bucket name to be read and destination Bigquery Table name.

It picks up the environment and product information from respective config files

job name for pipeline is generated based on dataset name and table name

@author: Rajnikant Rakesh

Arguments Reuired : 

Example - k//

--config "config.properties" --productconfig "ivans.properties" --env "dev"   --separator "|"  --stripheader "0" --stripdelim "0"  --addaudit "1"  --writeDeposition "WRITE_APPEND"  --system "IE" --input "gs://dw-dev-insurance/ivans/archive/IE_NCNU_20180227.DAT"

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



cwd = os.path.dirname(os.path.abspath(__file__))



if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"



else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"

    

def readschema():

    for seg in segments:

        if srcsystem =='AS400' and seg !='N1' :

            print("This is not a IE file")

        elif srcsystem =='IE': 

            tblname = srcsystem+"_"+seg+"_LDG"

            schema = readtableschema(env_config['projectid'], product_config['datasetname'], tblname)

            tablefields[seg]=schema    

    

class filter_segments(beam.DoFn):

    def process(self, element, segment):

        """This Function will Filter data based on segments passed""" 

        if element.find(segment,7,9)>0:

           yield element

        else:

            return 

        

def processsegments(inputdata, seg):

    

    if seg =='A1':

       col_specification =[0,7,9,12,15,20,30,40,46,56,59,65,75]

       row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

       del row[1]   

       row.insert(0,filedate )

       

       record='|'.join(row)

       logging.info('Processed record is ... %s', record )         

       return  record

    elif seg == 'B1':

         col_specification =[0,7,9,12,15,19,39,59,74,77,112,142,167,169,178,183,193]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         del row[1]   

         row.insert(0,filedate )

          

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

    

    elif seg == 'B2':

         col_specification =[0,7,9,12,15,19,39,59,74,77,112,142,167,169,178,183,193,203]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]   

         row.insert(0,filedate )

        

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

      

    elif seg == 'C1':

         col_specification =[0,7,9,12,15,27,31,46,56,66,76,86,88,99,144,146,157,175,186,197,227,239,248,249]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row[12]=int(row[12])   

         row[12]=row[12] / 100

         row[12]=str(row[12])

         row.pop()

         del row[1]

         row.insert(0,filedate )

         

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

    elif seg == 'C2':

         col_specification =[0,7,9,12,15,25,35,45,49,51,53,56,67,71,72,74,149,224]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         del row[1]

         row.insert(0,filedate )

         

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

     

    elif seg=='C3':    

         col_specification =[0,7,9,12,15,35,50,60]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

    elif seg == 'D1':

         col_specification =[0,7,9,12,15]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         del row[1]

         row.insert(0,filedate )



         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

    elif seg == 'E1':

         col_specification =[0,7,9,12,15,25,29,49,69,84,86,87,97,98,99,100,109,129,133,135,137,159,169,170,171,181] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop(15)

         row.pop(19)

         del row[1]

         row.insert(0,filedate )

        

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

    elif seg == 'G1':

         col_specification =[0,7,9,12,15,18,21,36,38,48] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         del row[1]

         row.insert(0,filedate )

     

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

    

    elif seg == 'H1':

         col_specification =[0,7,9,12,15,18,20,22,25] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         del row[1]

         row.insert(0,filedate )

        

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record

    

    elif seg == 'H2':

         col_specification =[0,7,9,12,15,45,80,110,135,137,146] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

        

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record 

     

    elif seg == 'I1':

         col_specification =[0,7,9,12,15,19,21,26,28,31,40,46,49,56,59,68,69,70,76,78,80,82,90,115,125,129,130,170,171,172,173,174,300]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record  

    elif seg == 'I2':

         col_specification =[0,7,9,12,15,19,22,23,28,31,32,37,42,44,47,50,51,58,69,73,77,81,82,83,84,85,86,87,88,89,90,93,104,105,107,110,111,112,114,119,123]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record    

     

    elif seg == 'I3':

         col_specification =[0,7,9,12,15,19,39,42,45,48,54,60,70,75,79,82,85,89,93,113,119,123,126,129,133,137,157,163,167,170,173,177,181,201,207,211,214,217,221,225,245,251,255,299,303,323,329]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         #row.pop(43)

         del row[1]

         row.insert(0,filedate )

        

         record='|'.join(row)

         logging.info('Processed record is ... %s', record ) 

         return  record 

    

    elif seg == 'J1':

         col_specification =[0,7,9,12,15,16,17,37,67,97,127,147,149,158] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

         #logging.info('Processed Row is ... %s', row ) 

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

     

    elif seg == 'K1':

         col_specification =[0,7,9,12,15,19,30,41,50,59,65,70,83,92,101] 

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         del row[1]

         row.insert(0,filedate )

         

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record 

     

    elif seg == 'L1':

         col_specification =[0,7,9,12,15,18,20]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

        

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

     

    elif seg == 'M1':

         col_specification =[0,7,9,12,15,21,30,80,105,110,112,117,120,122,127,132,141,144,146,148,152,154,204,208,218]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

     

         record='|'.join(row)

         logging.info('Processed record is ... %s', record )         

         return  record

    

    elif seg == 'N1':

         col_specification =[0,7,9,12,15,17,20,23,31,42]

         row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

         row.pop()

         del row[1]

         row.insert(0,filedate )

        

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

        

        numfields_in_row = tmp_row.count(separatorchar)-2*remove_first_last_delim +1

        numfields_in_table = len(tablefields[seg]) -2*add_audit_fields    

        if numfields_in_row == numfields_in_table:   

            for field in tablefields[seg]:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:   

                    rec[field] = None

                i=i+1

            return rec

        else :

            logging.info('Error Record is %s', validrow)

            return pvalue.TaggedOutput('error_records', validrow) 

                   

    except:

        logging.error("Error in processing row")

        raise





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

                     '--region', env_config['region'],

                     '--zone',env_config['zone'],

                     '--network',env_config['network'],

                     '--subnetwork',env_config['subnetwork'],

                     '--save_main_session', 'True',

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

        N1 = (readlines | 'Validate Segment N1' >> beam.ParDo(filter_segments(),segment='N1'))

        

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

        n1_valid_rows, error_rows = (N1 | 'Process Segments N1' >> beam.Map(processsegments,seg='N1').with_outputs('error_rows',main='valid_rows'))   

 

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

        n1_clean_rows, n1_error_records = n1_valid_rows | 'Cleanse Segment N1' >> beam.Map(rowextractor,seg='N1').with_outputs('error_records',main='valid_rows')

        

        

        a1_clean_rows | 'WriteBQ Segment A1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_A1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        b1_clean_rows | 'WriteBQ Segment B1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_B1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        b2_clean_rows | 'WriteBQ Segment B2'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_B2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        c1_clean_rows | 'WriteBQ Segment C1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_C1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        c2_clean_rows | 'WriteBQ Segment C2'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_C2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        c3_clean_rows | 'WriteBQ Segment C3'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_C3_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        d1_clean_rows | 'WriteBQ Segment D1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_D1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        e1_clean_rows | 'WriteBQ Segment E1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_E1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        g1_clean_rows | 'WriteBQ Segment G1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_G1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        h1_clean_rows | 'WriteBQ Segment H1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_H1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        h2_clean_rows | 'WriteBQ Segment H2'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_H2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        i1_clean_rows | 'WriteBQ Segment I1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_I1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        i2_clean_rows | 'WriteBQ Segment I2'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_I2_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        i3_clean_rows | 'WriteBQ Segment I3'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_I3_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                   ))

        

        j1_clean_rows | 'WriteBQ Segment J1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_J1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        k1_clean_rows | 'WriteBQ Segment K1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_K1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        l1_clean_rows | 'WriteBQ Segment L1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_L1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        m1_clean_rows | 'WriteBQ Segment M1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_M1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        

        n1_clean_rows | 'WriteBQ Segment N1'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"IE_N1_LDG"#+"$"+daily_load_params['tablepartitiondecorator']

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

        n1_error_records | 'Handler-ErrorRow N1'>> beam.Map(error_row_handler)

        

        p=pcoll.run()

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

    movefiles= "gsutil mv gs://"+product_config['bucket']+"/current/IE_NCNU_*"+" gs://"+product_config['bucket']+"/archive/"

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

        
