'''
Created on March 30, 2018
This module loads a Bigquery Table from Google Cloud Storage bucket.
Module needs bucket name to be read and destination Bigquery Table name.
It picks up the environment and product information from respective config files
job name for pipeline is generated based on dataset name and table name
@author: Rajnikant Rakesh
'''

import apache_beam as beam
from apache_beam import pvalue
from apache_beam import pvalue
import argparse
import logging
import os
import sys
from google.cloud import bigquery
from string import lower
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
from gettext import find

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

class filter_segments(beam.DoFn):
    def process(self, element, segment):
        """This Function will Filter data based on segments passed""" 
        tmp_seg="|"+segment+"|"
        seg=element.split(separatorchar)
        if seg[2]==segment:            
           element=element.replace(tmp_seg,'|')
           logging.info("Record %s Has %s " %(element,seg ))
           yield element
        else:
            return   
        
        
def read_files(pipeline):
    collections = []
    pcollectionlist =[]
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket(product_config['bucket'])
    #logging.info('Started Loading Files  ....')
    blob=bucket.list_blobs(prefix='ivans/current/')
    print(blob)
    
    for blob in bucket.list_blobs(prefix='ivans/current/IE'):
        try:
            filename=blob.name#.split('/')[1]
            print(filename)
            file="gs://"+product_config['bucket']+"/"+filename
            print(file)
            global segment
            global filedate
            segment=filename.split('/')[2].split('_')[1]
            filedate=filename.split('/')[2].split('_')[3].split('.')[0]
            collection = pipeline | ('Read  Segment  %s' % segment) >> beam.io.ReadFromText(file,skip_header_lines = remove_header_rows)
            collections.append(collection)
        except IOError:
            logging.error("Failed to read ")    
    
    return collections
''' 
def error_row_handler (error_row):
    logging.info("Entered error handler")
    numfields_in_row = error_row.count(separatorchar)-2*remove_first_last_delim +1
    numfields_in_table = len(tablefields) -2*add_audit_fields
    if numfields_in_row < numfields_in_table:
        logging.info("Row has too few fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row))
    else:
        logging.info("Row has too many fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row)) 
'''   
def rowextractor(validrow,tblname):
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
        #rowinfo = "Row Extractor: %d fields read,  %d in schema, Row string is %s"  % (len(row), len(tablefields), tmp_row)
        #logging.info(rowinfo)
        if(add_audit_fields):
            row.extend([job_timestamp,jobname])
            
        tablefields = readtableschema(env_config['projectid'], product_config['datasetname'], tblname)
        numfields_in_row = validrow.count(separatorchar)-2*remove_first_last_delim +1
        numfields_in_table = len(tablefields) -2*add_audit_fields
        #logstr = "Row has %d fields, Schema has %d, Row string is %s" % (numfields_in_row, numfields_in_table, inputdata)
        #logging.info(logstr)
    
        if numfields_in_row == numfields_in_table:
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
                     '--save_main_session', 'True',
                     '--num_workers', env_config['num_workers'],
                     '--max_num_workers', env_config['max_num_workers'],
                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],
                     '--service_account_name', env_config['service_account_name'],
                     '--service_account_key_file', env_config['service_account_key_file']
                     ]
    
    try:

        pcoll = beam.Pipeline(argv=pipeline_args)
        
        readfiles=read_files(pcoll)
        flatten=(readfiles | "Flatten Segments" >> beam.Flatten())
        
        A1 = (flatten | 'Validate Segment A1' >> beam.ParDo(filter_segments(),segment='A1'))
        B1 = (flatten | 'Validate Segment B1' >> beam.ParDo(filter_segments(),segment='B1'))
        B2 = (flatten | 'Validate Segment B2' >> beam.ParDo(filter_segments(),segment='B2'))
        C1 = (flatten | 'Validate Segment C1' >> beam.ParDo(filter_segments(),segment='C1'))
        C2 = (flatten | 'Validate Segment C2' >> beam.ParDo(filter_segments(),segment='C2'))
        C3 = (flatten | 'Validate Segment C3' >> beam.ParDo(filter_segments(),segment='C3'))
        D1 = (flatten | 'Validate Segment D1' >> beam.ParDo(filter_segments(),segment='D1'))
        E1 = (flatten | 'Validate Segment E1' >> beam.ParDo(filter_segments(),segment='E1'))
        G1 = (flatten | 'Validate Segment G1' >> beam.ParDo(filter_segments(),segment='G1'))
        H1 = (flatten | 'Validate Segment H1' >> beam.ParDo(filter_segments(),segment='H1'))
        H2 = (flatten | 'Validate Segment H2' >> beam.ParDo(filter_segments(),segment='H2'))
        I1 = (flatten | 'Validate Segment I1' >> beam.ParDo(filter_segments(),segment='I1'))
        I2 = (flatten | 'Validate Segment I2' >> beam.ParDo(filter_segments(),segment='I2'))
        I3 = (flatten | 'Validate Segment I3' >> beam.ParDo(filter_segments(),segment='I3'))
        J1 = (flatten | 'Validate Segment J1' >> beam.ParDo(filter_segments(),segment='J1'))
        K1 = (flatten | 'Validate Segment K1' >> beam.ParDo(filter_segments(),segment='K1'))
        L1 = (flatten | 'Validate Segment L1' >> beam.ParDo(filter_segments(),segment='L1'))
        M1 = (flatten | 'Validate Segment M1' >> beam.ParDo(filter_segments(),segment='M1'))
         
        if srcsystem    =="IE":
            N1 = (flatten | 'Validate Segment N1' >> beam.ParDo(filter_segments(),segment='N1'))
        
        clean_rows_A1 = A1 | 'Cleanse Segment A1' >> beam.Map(rowextractor,tblname=srcsystem+'_A1_LDG_1') 
        clean_rows_A1 | 'WriteBQ A1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_A1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        
        clean_rows_B1 = B1 | 'Cleanse Segment B1' >> beam.Map(rowextractor,tblname=srcsystem+'_B1_LDG_1') 
        clean_rows_B1 | 'WriteBQ B1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_B1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        '''
        clean_rows_B2 = B2 | 'Cleanse Segment B2' >> beam.Map(rowextractor,tblname=srcsystem+'_B2_LDG_1') 
        clean_rows_B2 | 'WriteBQ B2 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_B2_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_C1 = C1 | 'Cleanse Segment C1' >> beam.Map(rowextractor,tblname=srcsystem+'_C1_LDG_1') 
        clean_rows_C1 | 'WriteBQ C1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_C1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_C2 = C2 | 'Cleanse Segment C2' >> beam.Map(rowextractor,tblname=srcsystem+'_C2_LDG_1') 
        clean_rows_C2 | 'WriteBQ C2 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_C2_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
                 
        clean_rows_C3 = C3 | 'Cleanse Segment C3' >> beam.Map(rowextractor,tblname=srcsystem+'_C3_LDG_1') 
        clean_rows_C3 | 'WriteBQ C3 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_C3_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_D1= D1 | 'Cleanse Segment D1' >> beam.Map(rowextractor,tblname=srcsystem+'_D1_LDG_1') 
        clean_rows_D1 | 'WriteBQ D1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_D1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_E1= E1 | 'Cleanse Segment E1' >> beam.Map(rowextractor,tblname=srcsystem+'_E1_LDG_1') 
        clean_rows_E1 | 'WriteBQ E1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_E1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_G1= G1 | 'Cleanse Segment G1' >> beam.Map(rowextractor,tblname=srcsystem+'_G1_LDG_1') 
        clean_rows_G1 | 'WriteBQ G1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_G1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_H1= H1 | 'Cleanse Segment H1' >> beam.Map(rowextractor,tblname=srcsystem+'_H1_LDG_1') 
        clean_rows_H1 | 'WriteBQ H1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_H1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_H2= H2 | 'Cleanse Segment H2' >> beam.Map(rowextractor,tblname=srcsystem+'_H2_LDG_1') 
        clean_rows_H2 | 'WriteBQ H2 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_H2_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_I1= I1 | 'Cleanse Segment I1' >> beam.Map(rowextractor,tblname=srcsystem+'_I1_LDG_1') 
        clean_rows_I1 | 'WriteBQ I1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_I1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_I2= I2 | 'Cleanse Segment I2' >> beam.Map(rowextractor,tblname=srcsystem+'_I2_LDG_1') 
        clean_rows_I2 | 'WriteBQ I2 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_I2_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_I3= I3 | 'Cleanse Segment I3' >> beam.Map(rowextractor,tblname=srcsystem+'_I3_LDG_1') 
        clean_rows_I3 | 'WriteBQ I3 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_I3_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_J1= J1 | 'Cleanse Segment J1' >> beam.Map(rowextractor,tblname=srcsystem+'_J1_LDG_1') 
        clean_rows_J1 | 'WriteBQ J1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_J1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_K1= K1 | 'Cleanse Segment K1' >> beam.Map(rowextractor,tblname=srcsystem+'_K1_LDG_1') 
        clean_rows_K1 | 'WriteBQ K1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_K1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_L1= L1 | 'Cleanse Segment L1' >> beam.Map(rowextractor,tblname=srcsystem+'_L1_LDG_1') 
        clean_rows_L1 | 'WriteBQ L1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_L1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        clean_rows_M1= M1 | 'Cleanse Segment M1' >> beam.Map(rowextractor,tblname=srcsystem+'_M1_LDG_1') 
        clean_rows_M1 | 'WriteBQ M1 Segment'    >> beam.io.Write(
                beam.io.BigQuerySink(
                    product_config['datasetname']+"."+srcsystem+"_M1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                    ,write_disposition=writedeposition
                    ))
        
        if srcsystem=='IE':             
            clean_rows_N1= N1 | 'Cleanse Segment N1' >> beam.Map(rowextractor,tblname=srcsystem+'_N1_LDG_1') 
            clean_rows_N1 | 'WriteBQ N1 Segment'    >> beam.io.Write(
                    beam.io.BigQuerySink(
                        product_config['datasetname']+"."+srcsystem+"_N1_LDG_1"#+"$"+daily_load_params['tablepartitiondecorator']
                        ,write_disposition=writedeposition
                        ))
         '''       
        #error_rows | 'Handler-ErrorRow'>> beam.Map(error_row_handler)
        #Run the pipeline (all operations are deferred until run() is called).
        
        pcoll.run()
        #pcoll.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise
    
def main(config, productconfig, env, separator, stripheader, stripdelim, addaudit,system, writeDeposition='WRITE_TRUNCATE'):
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
    #global tablename
    global separatorchar 
    global remove_header_rows
    global remove_first_last_delim
    #global gssourcebucket
    global tablefields
    global writedeposition
    global add_audit_fields
    global job_timestamp
    global jobname
    global projectid
    global srcsystem
               
    if writeDeposition=='WRITE_TRUNCATE':
        writedeposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    else:
        writedeposition=beam.io.BigQueryDisposition.WRITE_APPEND
    
    tablefields = []
    save_main_session = True
    #tablename = output
    separatorchar = separator
    remove_header_rows = stripheader
    remove_first_last_delim = stripdelim
    add_audit_fields=addaudit
    #gssourcebucket = input
    ts = time.gmtime()
    job_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", ts)
    srcsystem=system

    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())
    global env_config
    env_config = readconfig(config, env )
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
  
    global product_config
    product_config = readconfig(productconfig, env)
    projectid=env_config['projectid']
    exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals())

    #if srcsystem =='AS400' :
    jobname = lower("load-" + product_config['productname']+"-" + srcsystem+"-segments-0713")
    #else:
    #    jobname = lower("load-" + product_config['productname']+ "-"+ srcsystem+"-segments")
 
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
         args.writeDeposition
         )
