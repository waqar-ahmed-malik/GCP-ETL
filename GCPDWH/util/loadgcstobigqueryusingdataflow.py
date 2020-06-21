'''
Created on March 30, 2018
This module loads a Bigquery Table from Google Cloud Storage bucket.
Module needs bucket name to be read and destination Bigquery Table name.
It picks up the environment and product information from respective config files
job name for pipeline is generated based on dataset name and table name
@author: Rajnikant Rakesh
'''

import apache_beam as beam
import argparse
import logging
import os
from google.cloud import bigquery
from string import lower
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

util_path = os.path.dirname(os.path.abspath(__file__))

def rowextractor(inputdata):
    """reads input and splits to relevant row format"""
    logging.info("Starting row processing in row extractor")
    try:
        rec = {}
        i=0
        row = inputdata.split(separatorchar)
        for field in tablefields:
            print(field)
            if row[i]:
                rec[field]= row[i].strip('"').strip('\n').strip('\r')
            else:
                rec[field]= None
            i=i+1
            #logging.info(field.name+":"+rec[field.name])
        #logging.info(rec)
    except:
        logging.error("Error in processing row")
        raise
    
    print(rec)
    print() 
    return rec
    
def run():
    """TBD"""
    jobname = lower("load-" + product_config['datasetname'].replace("_", "") + "-" + tablename.replace("_", "") )
    pipeline_args = ['--project', env_config['projectid'],
                     '--job_name', jobname,
                     '--runner', env_config['runner'],
                     '--staging_location', product_config['stagingbucket'],
                     '--temp_location', product_config['tempbucket'],
                     '--requirements_file', env_config['requirements_file'],
                     '--save_main_session', 'True',
                     '--num_workers', env_config['num_workers'],
                     '--max_num_workers', env_config['max_num_workers'],
                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],
                     '--service_account_name', env_config['service_account_name'],
                     '--service_account_key_file', env_config['service_account_key_file']
                     ]
    try:

        pcoll = beam.Pipeline(argv=pipeline_args)
        #readlines = pcoll | 'readRecords' >> beam.io.ReadFromText(gssourcebucket)
        readlines = pcoll | 'readRecords' >> beam.io.ReadFromText(gssourcebucket)
        print(readlines)
        rows = readlines | 'Select' >> beam.Map(rowextractor)
        print(rows)
        rows | 'Write'>>beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+tablename#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        # Run the pipeline (all operations are deferred until run() is called).
        pcoll.run()
    except:
        logging.exception('Failed to launch datapipeline')
        raise
    
def main(config, productconfig, env, input,  separator, output,writeDeposition='WRITE_APPEND'):
    global save_main_session 
    global tablename
    global separatorchar 
    global gssourcebucket
    global tablefields
    global writedeposition
    
    if writeDeposition=='WRITE_TRUNCATE':
        writedeposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    else:
        writedeposition=beam.io.BigQueryDisposition.WRITE_APPEND
    
    tablefields = []
    save_main_session = True
    tablename = output
    separatorchar = separator
    gssourcebucket = input
     #Read environment configuration
    exec(compile(open(util_path+'/readconfig.py').read(), util_path+'/readconfig.py', 'exec'), globals())
    global env_config
    env_config = readconfig(config, env )
    #Read product configuration
    global product_config
    product_config = readconfig(productconfig, env)
    exec(compile(open(util_path+'/readtableschema.py').read(), util_path+'/readtableschema.py', 'exec'), globals())
    tablefields = readtableschema(env_config['projectid'], product_config['datasetname'], tablename)
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

    main(args.config,
         args.productconfig,
         args.env,
         args.input,
         args.separator,
         args.output,
        # args.reportdate,
         args.writeDeposition)

    

