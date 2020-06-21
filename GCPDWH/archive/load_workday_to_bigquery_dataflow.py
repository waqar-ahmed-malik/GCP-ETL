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
import sys
import time
from google.cloud import bigquery
from string import lower,upper
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from datetime import datetime, timedelta
from google.cloud import storage


cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

def readfileasstring(sqlfile):
    """Read any text file and return text as a string. This function is used to read .sql and .schema files"""
    with open (sqlfile, "r") as file:
        sqltext=file.read().strip("\n").strip("\r")
    return sqltext

class load_csv_to_bigquery_using_bqsdk(beam.DoFn):
    def process (self,
                 context,
                 projectid,
                 datasetname,
                 csv_file_path,
                 outputTableName,
                 fieldDelimiter,
                 skipLeadingRows,
                 writeDeposition):

        """This function will read data from csv and load in BigQuery destination table using BigQuery sdk insert job."""
        logging.info('Loading table '+outputTableName)
        print('Loading table {}'.format(outputTableName))
        try:
            #credentials = GoogleCredentials.get_application_default()
            bigqclient = bigquery.Client(project=projectid)#, credentials=credentials)
            tdatasetname = bigqclient.dataset(datasetname)
            table_ref = tdatasetname.table(outputTableName)
            #table.reload()
            table = bigqclient.get_table(table_ref)
            tableschema = table.schema
#             outputTableSchema = []
#             for fields in tableschema:
#                 outputTableSchema.append(fields.name)
        except:
            logging.exception('Target table is not created ('+datasetname+"."+outputTableName+"). Please create it first.")
            raise RuntimeError('Target table is not created ('+datasetname+"."+outputTableName+"). Please create it first.")
        try:
            query_data = {
                'configuration':{
                            'load':{
                                'ignoreUnknownValues':False,
                                'skipLeadingRows':skipLeadingRows,
                                'sourceFormat':'CSV',
                                'destinationTable':{
                                    'projectId':projectid,
                                    'datasetId':datasetname,
                                    'tableId':outputTableName
                                    },
                                #'allowQuotedNewLines':True,
                                'encoding':'ISO-8859-1',
                                'maxBadRecords':100,
                                'allowJaggedRows':False,
                                'writeDisposition':writeDeposition,
                                'sourceUris':[csv_file_path],
                                'fieldDelimiter':fieldDelimiter
#                                 'schema':outputTableSchema
                                }
                            }
                }

            credentials = GoogleCredentials.get_application_default()
            bq = discovery.build('bigquery', 'v2', credentials=credentials)
            job=bq.jobs().insert(projectId=projectid,body=query_data).execute()
            logging.info('Waiting for job to finish...')
            request = bq.jobs().get(
            projectId=job['jobReference']['projectId'],
            jobId=job['jobReference']['jobId'])
            result = request.execute()
            while result['status']['state'] != 'DONE':
                result = request.execute()
            if result['status']['state'] == 'DONE':
                if 'errorResult' in result['status']:
                    for error in result['status']['errors']:
                        logging.exception('reason :'+error['reason']+',message :'+error['message'])
                    raise RuntimeError(result['status']['errorResult'])
                logging.info("Data loading completed successfully [Source:"+csv_file_path+", Destination:"+outputTableName+"]")
                print("Data loaded successfully [Source:{}, Destination:{}]".format(csv_file_path, outputTableName))
        except RuntimeError:
            raise
        return list("1")
class runbqsql(beam.DoFn):
    def process(self, context, inputquery):
          client = bigquery.Client(project=env_config['projectid'])
          query=inputquery
          query_job = client.query(query)
          results = query_job.result()
          print(results)
          for row in results:
              dummy=1
          return list("1")

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
                     '--requirements_file', '/home/airflow/gcs/data/GCPDWH/workday/requirements.txt',
                     '--save_main_session', 'True',
                     '--num_workers', env_config['num_workers'],
                     '--max_num_workers', env_config['max_num_workers'],
                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],
                     '--service_account_name', env_config['service_account_name'],
                     '--service_account_key_file', env_config['service_account_key_file']
                     ]

    try:

        collections = []
        pcoll = beam.Pipeline(argv=pipeline_args)
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])

        current_date=(datetime.today() - timedelta(days=1)).strftime("%m.%d.%Y")
        print(current_date)
        client = storage.Client(env_config['projectid'])
        bucket = client.bucket(product_config['bucket'])
        logging.info('Started Loading Files  ....')
        for blob in bucket.list_blobs(prefix='current/wd-'):
            if blob.name.endswith(current_date+".csv"):
                if not ( blob.name.find('club')>0 or  blob.name.find('timesheet')>0):
                    filename=blob.name#.split('/')[1]
                    targettable=upper(filename.split('/')[1].split('.')[0].replace('wd','WORKDAY').replace('-','_'))
                    fileid=filename.split('.')
                    del fileid[0]
                    del fileid[3]
                    fileids = [ fileid[2],fileid[0],fileid[1] ]
                    fileid=''.join(fileids)
                    file="gs://"+product_config['bucket']+"/"+filename
                    logging.info('Loading %s file into %s table ',filename.split('/')[1],targettable)
                    collection = dummy | ('Load %s' %targettable ) >> beam.ParDo(load_csv_to_bigquery_using_bqsdk(),projectid,datasetname,file,targettable,',',1,'WRITE_TRUNCATE')
                    collections.append(collection)


        flatten=(collections | 'Flatten Workday Employee' >> beam.Flatten())
        flatten_globally= flatten | 'Dummy : Flatten Globally Workday Employee' >> beam.CombineGlobally(any)

        current_wd_worker=(flatten_globally | 'Current Worker Dim..' >>  beam.ParDo(runbqsql(),curr_wd_worker))
        work_employee=(current_wd_worker | 'Employee Work Dim..' >>  beam.ParDo(runbqsql(),work_wd_employee.replace('V_FILE_ID',fileid)))
        upd_work_employee=(work_employee | 'Update Work Dim..' >>  beam.ParDo(runbqsql(),update_md5))
        emp_dim=(upd_work_employee | 'Employee Dim..' >>  beam.ParDo(runbqsql(),employee_dim.replace('V_JOB_RUN_ID',str(jobrunid)).replace('V_JOB_NAME',str(jobname))))
        upd_employee=(emp_dim | 'Update Employee Dim..' >>  beam.ParDo(runbqsql(),employee_dim_type2))
        emp_dim_final=(upd_employee | 'Load Employee Dim Final..' >>  beam.ParDo(runbqsql(),employee_dim_final))
        loc_dim=(flatten_globally | 'Location Dim..' >>  beam.ParDo(runbqsql(),location_dim.replace('V_FILE_ID',fileid)))
        upd_loc_dim=(loc_dim | 'Updating Location Dim..' >>  beam.ParDo(runbqsql(),location_upd))

        p = pcoll.run()
        p.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise

def main(config, productconfig, env):
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
    global datasetname
    global tablefields
    global writedeposition
    global add_audit_fields
    global job_timestamp
    global jobname
    global projectid
    global fileid
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)

    ts = time.gmtime()
    job_timestamp = time.strftime("%Y%m%d", ts)
    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())
    global env_config
    env_config = readconfig(config, env )

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']

    global product_config
    product_config = readconfig(productconfig, env)
    projectid=env_config['projectid']
    exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals())
    datasetname=product_config['datasetname']

    global curr_wd_worker
    global work_wd_employee
    global update_md5
    global employee_dim
    global employee_dim_type2
    global employee_dim_final
    global location_dim
    global location_upd

    curr_wd_worker=readfileasstring(cwd+"/sql/ins_cur_wd_employee.sql")
    work_wd_employee=readfileasstring(cwd+"/sql/ins_work_wd_employee.sql")
    update_md5=readfileasstring(cwd+"/sql/upd_work_wd_employee.sql")
    employee_dim=readfileasstring(cwd+"/sql/ins_employee_dim_type2.sql")
    employee_dim_type2=readfileasstring(cwd+"/sql/upd_employee_dim_type2.sql")
    employee_dim_final=readfileasstring(cwd+"/sql/load_employee_dim_final.sql")
    location_dim=readfileasstring(cwd+"/sql/ins_location_dim.sql")
    location_upd=readfileasstring(cwd+"/sql/upd_location_dim_type2.sql")

    jobname = lower("load-" + product_config['productname']+"-dwh-mart-" +job_timestamp)

    print(jobname)
    run()
   # wait_for_dataflow_job_finish(jobname)
    sys.exit()

if __name__ == '__main__':
    """Input -> Config file,
    product config file,
    env
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


    args = parser.parse_args()

    main(args.config,
         args.productconfig,
         args.env
         )