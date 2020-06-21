

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

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"



class load_csv_to_bigquery_using_bqsdk(beam.DoFn):    

    def process(self,

                context,

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

            outputTableSchema = []

            for fields in tableschema:

                outputTableSchema.append(fields.name)

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

                                'allowQuotedNewLines':True,

                                'encoding':'ISO-8859-1',

                                'maxBadRecords':20,

                                'allowJaggedRows':False,

                                'writeDisposition':writeDeposition,

                                'sourceUris':[csv_file_path],

                                'fieldDelimiter':fieldDelimiter,

                                'schema':outputTableSchema

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

#                     '--network',env_config['network'],

#                     '--subnetwork',env_config['subnetwork'],

                     '--save_main_session', 'True',

                     '--num_workers', env_config['num_workers'],

                     '--max_num_workers', env_config['max_num_workers'],

                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],

                     '--service_account_name', env_config['service_account_name'],

                     '--service_account_key_file', env_config['service_account_key_file']

                     ]

    

    try:



        pcoll = beam.Pipeline(argv=pipeline_args)

        dummy_row= pcoll | 'Initializing Pipeline' >> beam.Create(['1'])

        dummy_row | 'Ceabluec : Loading Work Table from csv' >> beam.ParDo(load_csv_to_bigquery_using_bqsdk(),'gs://propertycasualtyinsurance/GCP/CEABLUEC04162018.04.16.2018.csv','WORK_PCOMP_CEABLUEC',',',1,'WRITE_TRUNCATE')

        dummy_row | 'Flood : Loading Work Table from csv' >> beam.ParDo(load_csv_to_bigquery_using_bqsdk(),'gs://propertycasualtyinsurance/GCP/CSAALIST_06112018.csv','WORK_PCOMP_FLOOD',',',1,'WRITE_TRUNCATE')

        dummy_row | 'HomeIncentive : Loading Work Table from csv' >> beam.ParDo(load_csv_to_bigquery_using_bqsdk(),'gs://propertycasualtyinsurance/GCP/QryHomeIncentive041518.04.16.2018.csv','WORK_PCOMP_HOMEINCENTIVE',',',1,'WRITE_TRUNCATE')

        

        p=pcoll.run()

        p.wait_until_finish()

    

    except:

        logging.exception('Failed to launch datapipeline')

        raise    

        

def main(config, productconfig, env ):

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

    global projectid

    global srcsystem

    global segments

    global datasetname

    global jobname

    

    

    save_main_session = True

#    filedate=gssourcebucket.split('/')[5].split('_')[2].split('.')[0]

#    ts = time.gmtime()

#    job_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", ts)

    time.strftime("%Y%m%d")

#    srcsystem=system



    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())

    global env_config

    env_config = readconfig(config, env )

    

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']

  

    global product_config

    product_config = readconfig(productconfig, env)

    projectid=env_config['projectid']

    print(projectid)

    datasetname=product_config['datasetname']

   # execfile(utilpath+"readtableschema.py", globals())

    jobname = "load-" + product_config['productname']+"-staging-Tables"

    print(jobname)



    run()

    # wait_for_dataflow_job_finish(jobname)  

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

  

  

    args = parser.parse_args()

    main(args.config,

         args.productconfig,

         args.env       

         )
