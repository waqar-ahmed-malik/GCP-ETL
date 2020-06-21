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



from datetime import datetime

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

    #Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup]+"/util/"



class filter_recordtype(beam.DoFn):

    def process(self, element, recordtype):

        """This Function will Filter data based on segments passed""" 

        if element.find(recordtype)>0:

           yield element

        else:

            return 



def rowvalidator(inputdata,type):

    if type =='SUBMISSION':    

        numfields_in_table = len(tablefields) -1*add_audit_fields

    elif type =='SUMMARY':    

        numfields_in_table = len(tablefields_1) -1*add_audit_fields

    elif type =='TAXRECORD':    

        numfields_in_table = len(tablefields_2) -1*add_audit_fields    

    elif type =='TRANSACTN':    

        numfields_in_table = len(tablefields_3) -1*add_audit_fields

    elif type =='TXNPRICING':    

        numfields_in_table = len(tablefields_4) -1*add_audit_fields

    elif type =='CHARGEBACK':    

        numfields_in_table = len(tablefields_5) -1*add_audit_fields

    elif type =='ADJUSTMENT':    

        numfields_in_table = len(tablefields_6) -1*add_audit_fields

    elif type =='FEEREVENUE':    

        numfields_in_table = len(tablefields_7) -1*add_audit_fields

    numfields_in_row = inputdata.count(separatorchar)-2*remove_first_last_delim +1

        

   

    if numfields_in_row == numfields_in_table:

        return inputdata

    else:

        return pvalue.TaggedOutput('error_rows', inputdata)

     

def error_row_handler (error_row,type):

    logging.info("Entered error handler")

    if type =='SUBMISSION':

        numfields_in_table = len(tablefields) -1*add_audit_fields

    elif type =='SUMMARY':

        numfields_in_table = len(tablefields_1) -1*add_audit_fields   

    elif type =='TAXRECORD':

        numfields_in_table = len(tablefields_2) -1*add_audit_fields   

    elif type =='TRANSACTN':

        numfields_in_table = len(tablefields_3) -1*add_audit_fields   

    elif type =='TXNPRICING':

        numfields_in_table = len(tablefields_4) -1*add_audit_fields   

    elif type =='CHARGEBACK':

        numfields_in_table = len(tablefields_5) -1*add_audit_fields   

    elif type =='ADJUSTMENT':

        numfields_in_table = len(tablefields_6) -1*add_audit_fields   

    elif type =='FEEREVENUE':

        numfields_in_table = len(tablefields_7) -1*add_audit_fields        

    numfields_in_row = error_row.count(separatorchar)-2*remove_first_last_delim +1

    

    if numfields_in_row < numfields_in_table:

        logging.info("Row has too few fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row))

    else:

        logging.info("Row has too many fields %d, Expecting %d: %s" % (numfields_in_row, numfields_in_table,error_row)) 

   

def rowextractor(validrow,type):

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

            

        if type =='SUBMISSION':    

            for field in tablefields:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:

                    rec[field] = None

                i=i+1

        elif type =='SUMMARY':    

            for field in tablefields_1:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:

                    rec[field] = None

                i=i+1                

        elif type =='TAXRECORD':    

            for field in tablefields_2:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:

                    rec[field] = None

                i=i+1

        elif type =='TRANSACTN':    

            for field in tablefields_3:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:

                    rec[field] = None

                i=i+1

        elif type =='TXNPRICING':    

            for field in tablefields_4:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:

                    rec[field] = None

                i=i+1

        elif type =='CHARGEBACK':    

            for field in tablefields_5:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:

                    rec[field] = None

                i=i+1

        elif type =='ADJUSTMENT':    

            for field in tablefields_6:

                if row[i]:

                    rec[field] = row[i].strip()

                    if ((rec[field]== '(null)') or  (rec[field] == '()')):

                        rec[field] = '' 

                else:

                    rec[field] = None

                i=i+1

        elif type =='FEEREVENUE':    

            for field in tablefields_7:

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

        submission_records = (readlines | 'Read SUBMISSION RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'SUBMISSION' )) 

        valid_rows, error_rows = (submission_records | 'Validate SUBMISSION RecType' >> beam.Map(rowvalidator,'SUBMISSION').with_outputs('error_rows',main='valid_rows'))

        clean_rows = valid_rows | 'Cleanse SUBMISSION RecType' >> beam.Map(rowextractor,'SUBMISSION')

        clean_rows | 'WriteBQ SUBMISSION RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_SUBMISSION_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows | 'Handler-ErrorRow-SUBMISSION'>> beam.Map(error_row_handler,'SUBMISSION')

        

        summary_records = (readlines | 'Read SUMMARY RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'SUMMARY' )) 

        valid_rows_1, error_rows_1 = (summary_records | 'Validate SUMMARY RecType' >> beam.Map(rowvalidator,'SUMMARY').with_outputs('error_rows_1',main='valid_rows_1'))

        clean_rows_1 = valid_rows_1 | 'Cleanse SUMMARY RecType' >> beam.Map(rowextractor,'SUMMARY')

        clean_rows_1 | 'WriteBQ SUMMARY RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_SUMMARY_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows_1 | 'Handler-ErrorRow-SUMMARY'>> beam.Map(error_row_handler,'SUMMARY')

        

        summary_records = (readlines | 'Read TAXRECORD RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'TAXRECORD' )) 

        valid_rows_2, error_rows_2 = (summary_records | 'Validate TAXRECORD RecType' >> beam.Map(rowvalidator,'TAXRECORD').with_outputs('error_rows_2',main='valid_rows_2'))

        clean_rows_2 = valid_rows_2 | 'Cleanse TAXRECORD RecType' >> beam.Map(rowextractor,'TAXRECORD')

        clean_rows_2 | 'WriteBQ TAXRECORD RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_TAX_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows_2 | 'Handler-ErrorRow-TAXRECORD'>> beam.Map(error_row_handler,'TAXRECORD')      



        transactn_records = (readlines | 'Read TRANSACTN RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'TRANSACTN' )) 

        valid_rows_3, error_rows_3 = (transactn_records | 'Validate TRANSACTN RecType' >> beam.Map(rowvalidator,'TRANSACTN').with_outputs('error_rows_3',main='valid_rows_3'))

        clean_rows_3 = valid_rows_3 | 'Cleanse TRANSACTN RecType' >> beam.Map(rowextractor,'TRANSACTN')

        clean_rows_3 | 'WriteBQ TRANSACTN RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_TRANSACTION_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows_3 | 'Handler-ErrorRow-TRANSACTN'>> beam.Map(error_row_handler,'TRANSACTN')

        

        txnpricing_records = (readlines | 'Read TXNPRICING RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'TXNPRICING' )) 

        valid_rows_4, error_rows_4 = (txnpricing_records | 'Validate TXNPRICING RecType' >> beam.Map(rowvalidator,'TXNPRICING').with_outputs('error_rows_4',main='valid_rows_4'))

        clean_rows_4 = valid_rows_4 | 'Cleanse TXNPRICING RecType' >> beam.Map(rowextractor,'TXNPRICING')

        clean_rows_4 | 'WriteBQ TXNPRICING RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_TRANSACTION_PRICING_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows_4 | 'Handler-ErrorRow-TXNPRICING'>> beam.Map(error_row_handler,'TXNPRICING')

        

        chargeback_records = (readlines | 'Read CHARGEBACK RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'CHARGEBACK' )) 

        valid_rows_5, error_rows_5 = (chargeback_records | 'Validate CHARGEBACK RecType' >> beam.Map(rowvalidator,'CHARGEBACK').with_outputs('error_rows_5',main='valid_rows_5'))

        clean_rows_5 = valid_rows_5 | 'Cleanse CHARGEBACK RecType' >> beam.Map(rowextractor,'CHARGEBACK')

        clean_rows_5 | 'WriteBQ CHARGEBACK RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_CHARGEBACK_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows_5 | 'Handler-ErrorRow-CHARGEBACK'>> beam.Map(error_row_handler,'CHARGEBACK')



        adjustment_records = (readlines | 'Read ADJUSTMENT RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'ADJUSTMENT' )) 

        valid_rows_6, error_rows_6 = (adjustment_records | 'Validate ADJUSTMENT RecType' >> beam.Map(rowvalidator,'ADJUSTMENT').with_outputs('error_rows_6',main='valid_rows_6'))

        clean_rows_6 = valid_rows_6 | 'Cleanse ADJUSTMENT RecType' >> beam.Map(rowextractor,'ADJUSTMENT')

        clean_rows_6 | 'WriteBQ ADJUSTMENT RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_ADJUSTMENT_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows_6 | 'Handler-ErrorRow-ADJUSTMENT'>> beam.Map(error_row_handler,'ADJUSTMENT')



        feerevenue_records = (readlines | 'Read FEEREVENUE RecType' >> beam.ParDo(filter_recordtype(),recordtype= 'FEEREVENUE' )) 

        valid_rows_7, error_rows_7 = (feerevenue_records | 'Validate FEEREVENUE RecType' >> beam.Map(rowvalidator,'FEEREVENUE').with_outputs('error_rows_7',main='valid_rows_7'))

        clean_rows_7 = valid_rows_7 | 'Cleanse FEEREVENUE RecType' >> beam.Map(rowextractor,'FEEREVENUE')

        clean_rows_7 | 'WriteBQ FEEREVENUE RecType'    >> beam.io.Write(

                    beam.io.BigQuerySink(

                    product_config['datasetname']+".WORK_AMEX_FEES_AND_REVENUES_RECORD"#+"$"+daily_load_params['tablepartitiondecorator']

                    ,write_disposition=writedeposition

                    ))

        error_rows_7 | 'Handler-ErrorRow-FEEREVENUE'>> beam.Map(error_row_handler,'FEEREVENUE')

          

       # Run the pipeline (all operations are deferred until run() is called).

        p = pcoll.run()

        p.wait_until_finish()

    except:

        logging.exception('Failed to launch datapipeline')

        raise

    

def main(config, productconfig, env, input,  separator, stripheader, stripdelim, addaudit,writeDeposition ,segment_n):#,  output

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

#     global tablename

    global separatorchar 

    global remove_header_rows

    global remove_first_last_delim

    global gssourcebucket

    global tablefields

    global tablefields_1

    global tablefields_2

    global tablefields_3

    global tablefields_4

    global tablefields_5

    global tablefields_6

    global tablefields_7

    global writedeposition

    global add_audit_fields

    global jobname

    global projectid

    global fileid

    global segment_n_type

               

    if writeDeposition=='WRITE_TRUNCATE':

        writedeposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

    else:

        writedeposition=beam.io.BigQueryDisposition.WRITE_APPEND

    

    tablefields = []

    tablefields_1 = []

    tablefields_2 = []

    tablefields_3 = []

    tablefields_4 = []

    tablefields_5 = []

    tablefields_6 = []

    tablefields_7 = []

    save_main_session = True

#     tablename = output

    separatorchar = separator

    remove_header_rows = stripheader

    remove_first_last_delim = stripdelim

    add_audit_fields=addaudit

    gssourcebucket = input

    

    vfileid= gssourcebucket.split('/')[5].split('.')[2][0:6]

    fileid=datetime.strptime(vfileid,"%y%m%d").strftime("%m%d%y")

    segment_n_type = segment_n

    #fileid= vfile.split('.')[5]+vfile.split('.')[6]+vfile.split('.')[7]



    

    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals())

    global env_config

    env_config = readconfig(config, env )

    

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']

  

    global product_config

    product_config = readconfig(productconfig, env)

    projectid=env_config['projectid']

    exec(compile(open(utilpath+"readtableschema.py").read(), utilpath+"readtableschema.py", 'exec'), globals())

    tablefields = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_SUBMISSION_RECORD')

    print ("Tablefields are")

    print(tablefields)

    tablefields_1 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_SUMMARY_RECORD')

    print ("Tablefields_1 are")

    print(tablefields_1)    

    tablefields_2 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_TAX_RECORD')

    print ("Tablefields_2 are")

    print(tablefields_2)    

    tablefields_3 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_TRANSACTION_RECORD')

    print ("Tablefields_3 are")

    print(tablefields_3)    

    tablefields_4 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_TRANSACTION_PRICING_RECORD')

    print ("Tablefields_4 are")

    print(tablefields_4)    

    tablefields_5 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_CHARGEBACK_RECORD')

    print ("Tablefields_5 are")

    print(tablefields_5)

    tablefields_6 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_ADJUSTMENT_RECORD')

    print ("Tablefields_6 are")

    print(tablefields_6)

    tablefields_7 = readtableschema(env_config['projectid'], product_config['datasetname'], 'WORK_AMEX_FEES_AND_REVENUES_RECORD')

    print ("Tablefields_7 are")

    print(tablefields_7)

    

    jobname = "load-" + product_config['productname']+"-to-"+ product_config['datasetname']+fileid.replace('.csv','')

 

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

 

#     parser.add_argument(

#         '--output',

#         required=True,

#         help= ('Target BigQuery table for loading data specified as tablename')

#       )

    

    parser.add_argument(

        '--writeDeposition',

        required=False,

        default='WRITE_APPEND',

        help= ('WRITE_APPEND by Default, WRITE_TRUNCATE to truncate')

      )

    parser.add_argument(

        '--segment_n',

        required=False,

        default='SUMMARY',

        help= ('segemnt name of file')

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

         args.writeDeposition,

        args.segment_n#,args.output

         )
