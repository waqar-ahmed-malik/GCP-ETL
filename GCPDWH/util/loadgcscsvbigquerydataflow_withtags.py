'''
Created on March 30, 2018
This module loads a Bigquery Table from Google Cloud Storage bucket.
Module needs bucket name to be read and destination Bigquery Table name.
It picks up the environment and product information from respective config files
job name for pipeline is generated based on dataset name and table name
@author: Rajnikant Rakesh

Revision : 6/13

1. Added exception handling for bad records
2. util path added for relative directory
3. GCP Project enviornment configration externally defined.
'''

import apache_beam as beam
from apache_beam import pvalue
import argparse
import logging
import os
from google.cloud import bigquery
from string import lower
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time

util_path = os.path.dirname(os.path.abspath(__file__))

def rowvalidator(inputdata):
        logging.info("Starting row validator")
        numfields_in_row = inputdata.count(separatorchar)-2*remove_first_last_delim +1
        numfields_in_table = len(tablefields) -2*add_audit_fields
        logstr = "Row has %d fields, Schema has %d, Row string is %s" % (numfields_in_row, numfields_in_table, inputdata)
        logging.info(logstr)
    
        if numfields_in_row == numfields_in_table:
            #logging.info("Valid row")
            #return inputdata.encode("ISO-8859-1")
            return inputdata
        else:
            # logging.info("Error in row, wrong field count")
            #return pvalue.TaggedOutput('error_rows', inputdata.encode("ISO-8859-1"))
            return pvalue.TaggedOutput('error_rows', inputdata)
    
        
     
def error_row_handler (brokenrow):
   logging.info("Entered error handler for broken row: %s" % brokenrow)

  
   
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
       # rowinfo = "Row has %d fields,  Schema has %d, Row string is %s"  % (len(row), len(tablefields), tmp_row)
       # logging.info(rowinfo)
        if(add_audit_fields):
            row.extend([job_timestamp,jobname])
            
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
    1. Set Dataflow Pipeline configurations.
    2. Define Pclollection object
    3. Read lines from the csv file on GCS.
    4. Pass cleaned records to BigQuery table.
    5. Pass error records to error handler. 
    """
    #jobname = lower("Load-" + product_config['datasetname'].replace("_", "") + "-" + tablename.replace("_", "") )
    pipeline_args = ['--project', env_config['projectid'],
                     '--job_name', jobname,
                     '--runner', env_config['runner'],
                     '--staging_location', product_config['stagingbucket'],
                     '--temp_location', product_config['tempbucket'],
 #                    '--requirements_file', env_config['requirements_file'],
                     '--save_main_session', 'True',
                     '--num_workers', env_config['num_workers'],
                     '--max_num_workers', env_config['max_num_workers'],
                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],
                     '--service_account_name', env_config['service_account_name'],
                     '--service_account_key_file', env_config['service_account_key_file']
                     ]
    try:

        pcoll = beam.Pipeline(argv=pipeline_args)
        readlines = pcoll | 'readRecords' >> beam.io.ReadFromText(gssourcebucket,skip_header_lines = remove_header_rows)
        good_rows, bad_rows = (readlines | 'Validate' >> beam.Map(rowvalidator).with_outputs('error_rows', main='valid_rows'))
        clean_rows = good_rows | 'Cleanse' >> beam.Map(rowextractor)
        clean_rows | 'WriteBQ'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+tablename
                    ,write_disposition=writedeposition
                    ))
        bad_rows | 'Handler'>> beam.Map(error_row_handler)
        # Run the pipeline (all operations are deferred until run() is called).
        pcoll.run()
    except:
        logging.exception('Failed to launch datapipeline')
        raise
    
def main(config, productconfig, env, input,  separator, stripheader, stripdelim, addaudit,  output,writeDeposition='WRITE_APPEND'):
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
    ts = time.gmtime()
    job_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", ts)
  
    #Read environment configuration
    exec(compile(open(util_path+'/readconfig.py').read(), util_path+'/readconfig.py', 'exec'), globals())
    global env_config
    env_config = readconfig(config, env )
    #Read product configuration
    global product_config
    product_config = readconfig(productconfig, env)
    exec(compile(open(util_path+'/readtableschema.py').read(), util_path+'/readtableschema.py', 'exec'), globals())
    tablefields = readtableschema(env_config['projectid'], product_config['datasetname'], tablename)
    jobname = lower("Load1-" + product_config['datasetname'].replace("_", "") + "-" + tablename.replace("_", "") )
    #execfile('C://GCP//GCPDWH//util//setdailyloadparams.py', globals())
   # global daily_load_params
    #daily_load_params = setdailyloadparams(product_config['timezoneoffset'],reportdate)
    #Launch the pipeline
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
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
    '''
    parser.add_argument(
        '--jobowner',
        required=False,
        default=' ',
        help= ('Initials(optional) of the person running this job')
        )
    '''    
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
    parser.add_argument(
        '--reportdate',
        required=False,
        default=None,
        help= ('Report date[Optional] for which report is running, else it will run for last day')
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
        #args.jobowner,
         args.output,
        # args.reportdate,
         args.writeDeposition)