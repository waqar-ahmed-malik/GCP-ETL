'''

Created on August 03, 2018

This module loads claims files from the storage buckets into Claims Work tables using dataflow pipelines.

This modules also add a audit field called FILE_ID to the table which is the actual file date (YYYYMMDD) loaded in the landing table.



The sql's in the same folder then transfers the data into Insurance fact and Insurance customer tables.

@author: Rajnikant Rakesh



Arguments Required : 

Eg :

--config "config.properties" --productconfig "lifeinsurance.properties" --env "dev"  --input "gs://dw-prod-claims/current/CLAIMS_NCNU_20160101_080040.txt.01.01.2016" --separator "|"  --stripheader "1" --stripdelim "0"  --addaudit "1"   --output "WORK_INSURANCE_CLAIMS"   --writeDeposition "WRITE_TRUNCATE"  



'''



import apache_beam as beam

from apache_beam import pvalue

from apache_beam import coders

import argparse

import logging

import os

import sys

from google.cloud import bigquery

from googleapiclient import discovery

from oauth2client.client import GoogleCredentials

import time



cwd = os.path.dirname(os.path.abspath(__file__))



if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup]+"\\util\\"

    

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"





def rowvalidator(inputdata):

        numfields_in_row = inputdata.count(separatorchar)-2*remove_first_last_delim +1

        numfields_in_table = len(tablefields) -1*add_audit_fields

   

        if numfields_in_row == numfields_in_table:

            return inputdata

        else:

            return pvalue.TaggedOutput('error_rows', inputdata)

     

def error_row_handler (error_row):

    logging.info("Entered error handler")

    numfields_in_row = error_row.count(separatorchar)-2*remove_first_last_delim +1

    numfields_in_table = len(tablefields) -1*add_audit_fields

    if numfields_in_row < numfields_in_table:

        logging.info("Row has too few fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row))

    else:

        logging.info("Row has too many fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row)) 

   

def rowextractor(validrow):

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

            row.extend([fileid])

            

        for field in tablefields:

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

                    '--requirements_file', env_config['requirements_file'],

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

        readlines = pcoll | 'Read Records' >> beam.io.ReadFromText(gssourcebucket,skip_header_lines = remove_header_rows) 

        valid_rows, error_rows = (readlines | 'Validate Travel TST' >> beam.Map(rowvalidator).with_outputs('error_rows',main='valid_rows'))

        clean_rows = valid_rows | 'Cleanse TST' >> beam.Map(rowextractor)

        clean_rows | 'WriteBQ TST'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+"."+tablename#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows | 'Handler-ErrorRow'>> beam.Map(error_row_handler)

       # Run the pipeline (all operations are deferred until run() is called).

        pcoll.run()

#        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise

    

def main(config, productconfig, env, input,  separator, stripheader, stripdelim, addaudit,  output,writeDeposition ):

    """

    1. Define required global variables. 

    2. Set Deposition method to WRITE_TRUNCATE in case not specified tough it is required parameter.

    3. Generate stream for Audit fields in data. i.e. FILE_ID = {filedate in (YYYYMMDD format}

    4. Read config.properties by calling readconfig method from readconfig.py file

    5. Authenticate GCP project, read json file from config file. (This is explict authentication)

    6. Read BigQuery Table schema using readshcema method from readschema.py file.

    7. Call DataFlow PipeLine method, this initiate Dataflow job on GCP.

    

    """

    global save_main_session 

    global tablename

    global separatorchar 

    global remove_header_rows

    global remove_first_last_delim

    global gssourcebucket

    global tablefields

    global writedeposition

    global add_audit_fields

    global jobname

    global projectid

    global fileid

               

    if writeDeposition=='WRITE_TRUNCATE':

        writedeposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

    else:

        writedeposition=beam.io.BigQueryDisposition.WRITE_APPEND

    

    tablefields = []

    save_main_session = True

    tablename = output

    separatorchar = separator

    remove_header_rows = stripheader

    remove_first_last_delim = stripdelim

    add_audit_fields=addaudit

    gssourcebucket = input

    fileid=gssourcebucket.split('/')[5].split('_')[2].split('.')[0]

    print(fileid)

    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())

    global env_config

    env_config = readconfig(config, env )

    

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']

  

    global product_config

    product_config = readconfig(productconfig, env)

    projectid=env_config['projectid']

    exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals())

    tablefields = readtableschema(env_config['projectid'], product_config['datasetname'], tablename)

    print ("Tablefields are")

    print(tablefields)

    jobname = "load-" + product_config['productname']+"-to-"+ product_config['datasetname'] +"-"+fileid

 

    print(jobname)

    run()



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

        required=True,

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

        required=True,

        help= ('Target BigQuery table for loading data specified as tablename')

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

         args.input,

         args.separator,

         args.stripheader,

         args.stripdelim,

         args.addaudit,

         args.output,

         args.writeDeposition)

