'''

Created on July 11, 2018

This module loads a Bigquery Table from Google Cloud Storage bucket.

Module needs bucket name to be read and destination Bigquery Table name.

It picks up the environment and product information from respective config files

job name for pipeline is generated based on dataset name and table name

@author: Rajnikant Rakesh

Arguments Reuired : 

Example - k//

--config "config.properties" --productconfig "discover.properties" --env "dev"   --separator "|"  --stripheader "1" --stripdelim "0"  --addaudit "0"  --writeDeposition "WRITE_APPEND" --input "gs://dw-dev-inbound/discover-payment-transactions/current/AAAMWGO.MERSDLY.J2019153.N02380531048160"

@modified: Maniratnam Patchigolla

'''



import apache_beam as beam

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

    

    

class filter_segments(beam.DoFn):

    def process(self, element, recordtype):

        """This Function will Filter data based on segments passed""" 

        if element.find(recordtype)>0:

           yield element

        else:

            return 

        

def processsegments(inputdata, seg):

    

    if seg =='13D' :

        col_specification = [0,1,3,4,20,28,44,45,61,80,81,93,116,131,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields) -1*add_audit_fields

        return  record   

     

    elif seg =='13H' :

        col_specification = [0,1,3,4,19,27,35,51,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields_1) -1*add_audit_fields

        return  record

    

    elif seg =='08OB' :

        col_specification = [0,1,3,4,5,13,21,37,77,87,147,181,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields_2) -1*add_audit_fields

        return  record

 

    elif seg =='08OD' :

        col_specification = [0,1,3,4,5,13,21,37,53,113,129,163,182,183,206,221,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields_3) -1*add_audit_fields

        return  record

    

    numfields_in_row = inputdata.count(separatorchar)-2*remove_first_last_delim +1

        

    if numfields_in_row == numfields_in_table :

        return inputdata

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

            row.extend([job_timestamp])

           

        if seg == '13D' :

            for field in tablefields:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:   

                    rec[field] = None

                i=i+1

            return rec

        

        elif seg == '13H' :

            for field in tablefields_1:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:   

                    rec[field] = None

                i=i+1

            return rec

        

        elif seg == '08OB' :

            for field in tablefields_2:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:   

                    rec[field] = None

                i=i+1

            return rec

        

        elif seg == '08OD' :

            for field in tablefields_3:

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





def error_row_handler (error_row,seg):

    logging.info("Entered error handler")

    if seg =='13D' :

        col_specification = [0,1,3,4,20,28,44,45,61,80,81,93,116,131,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields) -1*add_audit_fields

        return  record    



    elif seg =='13H' :

        col_specification = [0,1,3,4,19,27,35,51,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields_1) -1*add_audit_fields

        return  record    



    elif seg =='08OB' :

        col_specification = [0,1,3,4,5,13,21,37,77,87,147,181,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields_2) -1*add_audit_fields

        return  record



    elif seg =='08OD' :

        col_specification = [0,1,3,4,5,13,21,37,53,113,129,163,182,183,206,221,335]

        row = [inputdata[i:j] for i,j in zip(col_specification, col_specification[1:]+[None])]

        record='|'.join(row)

        logging.info('Processed record is ... %s', record ) 

        numfields_in_table = len(tablefields_3) -1*add_audit_fields

        return  record



    numfields_in_row = inputdata.count(separatorchar)-2*remove_first_last_delim +1

        

    if numfields_in_row < numfields_in_table :

        logging.info("Row has too few fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row))

    else :

        logging.info("Row has too many fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row)) 

        

   

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

           

        

        Payment_Detail = (readlines | 'Validate Payment_Detail records' >> beam.ParDo(filter_segments(),recordtype='13D'))

        Payment_Detail_valid_rows, Payment_Detail_error_rows = (Payment_Detail | 'Process Payment_Detail records' >> beam.Map(processsegments,seg='13D').with_outputs('error_rows',main='valid_rows'))

        Payment_Detail_clean_rows = Payment_Detail_valid_rows | 'Cleanse Payment_Detail records' >> beam.Map(rowextractor,seg='13D')

        Payment_Detail_clean_rows | 'WriteBQ Payment_Detail records'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"WORK_DISCOVER_PAYMENT_DETAILS"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        Payment_Detail_error_rows | 'Handler-ErrorRow Payment_Detail'>> beam.Map(error_row_handler,'13D')  

               

        Payment_Batch= (readlines | 'Validate Payment_Batch records' >> beam.ParDo(filter_segments(),recordtype='13H'))

        Payment_Batch_valid_rows, Payment_Batch_error_rows = (Payment_Batch | 'Process Payment_Batch records' >> beam.Map(processsegments,seg='13H').with_outputs('error_rows_1',main='valid_rows_1'))

        Payment_Batch_clean_rows = Payment_Batch_valid_rows | 'Cleanse Payment_Batch records' >> beam.Map(rowextractor,seg='13H')

        Payment_Batch_clean_rows | 'WriteBQ Payment_Batch records'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"WORK_DISCOVER_PAYMENT_RECORDS"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        Payment_Batch_error_rows | 'Handler-ErrorRow Payment_Batch'>> beam.Map(error_row_handler,'13H')



        Failure_Batch= (readlines | 'Validate Failure_Batch records' >> beam.ParDo(filter_segments(),recordtype='08OB'))

        Failure_Batch_valid_rows, Failure_Batch_error_rows = (Failure_Batch | 'Process Failure_Batch records' >> beam.Map(processsegments,seg='08OB').with_outputs('error_rows_2',main='valid_rows_2'))

        Failure_Batch_clean_rows = Failure_Batch_valid_rows | 'Cleanse Failure_Batch records' >> beam.Map(rowextractor,seg='08OB')

        Failure_Batch_clean_rows | 'WriteBQ Failure_Batch records'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"WORK_DISCOVER_PAYMENT_FAILURE_RECORDS"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        Failure_Batch_error_rows | 'Handler-ErrorRow Failure_Batch'>> beam.Map(error_row_handler,'08OB')

 

        Failure_Detail= (readlines | 'Validate Failure_Detail records' >> beam.ParDo(filter_segments(),recordtype='08OD'))

        Failure_Detail_valid_rows, Failure_Detail_error_rows = (Failure_Detail | 'Process Failure_Detail records' >> beam.Map(processsegments,seg='08OD').with_outputs('error_rows_3',main='valid_rows_3'))

        Failure_Detail_clean_rows = Failure_Detail_valid_rows | 'Cleanse Failure_Detail records' >> beam.Map(rowextractor,seg='08OD')

        Failure_Detail_clean_rows | 'WriteBQ Failure_Detail records'    >> beam.io.Write(

                beam.io.BigQuerySink(

                    product_config['datasetname']+"."+"WORK_DISCOVER_PAYMENT_FAILURE_DETAILS"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        Failure_Detail_error_rows | 'Handler-ErrorRow Failure_Detail'>> beam.Map(error_row_handler,'08OD')

                

        p=pcoll.run()

        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise

    

def main(config, productconfig, env, separator, stripheader, stripdelim, addaudit,input, writeDeposition):

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

    global tablefields_1

    global tablefields_2

    global tablefields_3

    global writedeposition

    global add_audit_fields

    global job_timestamp

    global jobname

    global projectid

    global filedate

           

    if writeDeposition=='WRITE_TRUNCATE':

        writedeposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

    else:

        writedeposition=beam.io.BigQueryDisposition.WRITE_APPEND

    

    tablefields = {}

    tablefields_1 ={}

    tablefields_2 = {}

    tablefields_3 = {}

    

    save_main_session = True

    separatorchar = separator

    remove_header_rows = stripheader

    remove_first_last_delim = stripdelim

    add_audit_fields=addaudit

    gssourcebucket = input

    filedate=gssourcebucket.split('/')[5].split('.')[2][1:]

    ts = time.gmtime()

    job_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", ts)

    time.strftime("%Y%m%d")



    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())

    global env_config

    env_config = readconfig(config, env )

    

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']

  

    global product_config

    product_config = readconfig(productconfig, env)

    projectid=env_config['projectid']

    exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals())

    tablefields = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_DISCOVER_PAYMENT_DETAILS')

    print ("Tablefields are")

    print(tablefields)



    tablefields_1 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_DISCOVER_PAYMENT_RECORDS')

    print ("Tablefields_1 are")

    print(tablefields_1)



    tablefields_2 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_DISCOVER_PAYMENT_FAILURE_RECORDS')

    print ("Tablefields_2 are")

    print(tablefields_2)

 

    tablefields_3 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_DISCOVER_PAYMENT_FAILURE_DETAILS')

    print ("Tablefields_3 are")

    print(tablefields_3)

        

    jobname = "load" + product_config['productname']+"-"+product_config['datasetname'] 

    print(jobname)

    run()



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

        '--writeDeposition',

        required=False,

        default='WRITE_APPEND',

        help= ('WRITE_APPEND by Default, WRITE_TRUNCATE to truncate')

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

         args.input,

         args.writeDeposition         

         )

        
