'''
Created on April 09, 2018
@author: Rajnikant Rakesh
This module runs a dataflow pipeline.
It reads data from a bigquery table via sqlinput parameter and
loads data to a bigquery table via outputtable parameter.
It depends on config.properties and product config properties for details of load
'''

import apache_beam as beam
import argparse
import logging
from string import lower
import time

util_path = os.path.dirname(os.path.abspath(__file__))

def readsql(sqlfile):
    global sqltext
    f= open(sqlfile)
    sqlstring = f.read()
    sqltext = sqlstring.strip("\n").strip("\r")
    
def run():
    #global jobname
    #jobname = lower("load" + product_config['datasetname'].replace("_", "-") + "-" + tablename.replace("_", "") )
    pipeline_args = ['--project', env_config['projectid'],
                     '--job_name', jobname,
                     '--runner', env_config['runner'],
                     '--staging_location', product_config['stagingbucket'],
                     '--temp_location', product_config['tempbucket'],
                     '--requirements_file', env_config['requirements_file'],
                     '--save_main_session', 'True',
                     '--num_workers', env_config['num_workers'],
                     '--max_num_workers', env_config['max_num_workers'],
                     '--autoscaling_algorithm', env_config['autoscaling_algorithm']
                     ]
    
    pcoll = beam.Pipeline(argv=pipeline_args)
    #evalsqltext=
    evalsqltext = sqltext.replace('v_jobname',jobname)
    logging.info('Executing SQL {}'.format(sqltext))
    readlines = pcoll | 'readRecordsFromBigQueryTable' >> beam.io.Read(
                                              beam.io.BigQuerySource(query=evalsqltext, use_standard_sql=True ))
    
    logstr = "Row string is %s" % (readlines)
    logging.info(logstr)
    
    readlines | 'writeRecordsToBigQueryTable'>>beam.io.Write(
        beam.io.BigQuerySink(
            product_config['datasetname']+"."+tablename,#+"$"+daily_load_params['tablepartitiondecorator'],
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            ))
    logging.info('Launching pipeline for {} '.format(product_config['datasetname']+"."+tablename))
    pcoll.run()
    
def main(config, productconfig, env, sqlinput, outputtable, reportdate=None):
    global save_main_session 
    global tablename
    global job_timestamp
    global jobname
    save_main_session = True
    tablename = outputtable
    #Read environment configuration
    exec(compile(open(util_path+'/readconfig.py').read(), util_path+'/readconfig.py', 'exec'), globals())
    global env_config
    env_config = readconfig(config, env )
    #Read product configuration
    global product_config
    product_config = readconfig(productconfig, env)
    readsql(sqlinput)
    ts = time.gmtime()
    job_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", ts)
    jobname = lower("load" +'-'+ product_config['datasetname'].replace("_", "-") + "-" + tablename.replace("_", "") )      
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
    #Launch the pipeline
    run()
    
if __name__ == '__main__':
    #Input -> Config file with path, product config file with path, env, gcssourcefilename, tablename
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
        '--sqlinput',
        required=True,
        help= ('SQL file name with full path, if in a separate directory')
        )
    parser.add_argument(
        '--outputtable',
        required=True,
        help= ('Target BigQuery table for loading data specified as tablename')
      )
    parser.add_argument(
        '--reportdate',
        required=False,
        help= ('Date for which data is to be loaded in format YYYYMMDD')
      )
    args = parser.parse_args()
    main(args.config, 
         args.productconfig, 
         args.env, 
         args.sqlinput, 
         args.outputtable, 
         args.reportdate)

