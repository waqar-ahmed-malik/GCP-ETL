'''
Created on Jun 13, 2019
@author: Atul Guleria
This module reads Incremental ERS data from Oracle and Write into Bigquery Table
It uses pandas dataframe to read and write data to BigQuery.
'''

 
import apache_beam as beam
import time
import jaydebeapi 
import os
import argparse
from google.cloud import bigquery
import loggingimport base64
import sys
from google.cloud import storage
from google.cloud.storage import Blob
from datetime import datetime
import pandas as  pd
from oauth2client.client import GoogleCredentials
print((logging.__file__))

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

def readfileasstring(sqlfile):   
    """
    Read any text file and return text as a string. 
    This function is used to read .sql and .schema files
    """ 
    with open (sqlfile, "r") as file:
         sqltext=file.read().strip("\n").strip("\r")
    return sqltext

class setenv(beam.DoFn): 
      def process(self,context):
          os.system('gsutil cp '+product_config['stagingbucket']+'/ojdbc6.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )
          logging.info('Jar copied to Instance..')
          logging.info('Java Libraries copied to Instance..')
          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')
          logging.info('Enviornment Variable set.')
          return list("1")
          

class readfromoracle(beam.DoFn): 
      def process(self, context, inpquery,targettable):
          database_user=env_config[connectionprefix+'_database_user']
#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')          database_password = env_config[connectionprefix + '_database_password']          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')
          database_host=env_config[connectionprefix+'_database_host']
          database_port=env_config[connectionprefix+'_database_port']
          database_db=env_config[connectionprefix+'_database']
          
          jclassname = "oracle.jdbc.driver.OracleDriver"
          url = ("jdbc:oracle:thin:"+database_user+"/"+database_password+"@"+database_host +":"+database_port+"/"+database_db)
          jars = ["/tmp/ojdbc6.jar"]
          libs = None
          cnx = jaydebeapi.connect(jclassname, url, jars=jars,
                            libs=libs)   
          logging.info('Connection Successful..') 
          cnx.cursor()
          logging.info('Reading Sql Query from the file...')
          
          query = inpquery.replace('jobrunid',str(jobrunid)).replace('jobname', jobname).replace('incr_date_reim',incrementaldate_reim).replace('incr_date_d2k_audit',incrementaldate_d2k).replace('incr_date_svc_vehicle',incrementaldate_cad_svc_veh).replace('incr_date_aaa_referral',incrementaldate_aar_referral).replace('incr_date_ent_periods',incrementaldate_enti_period).replace('incr_date_tow_dest',incrementaldate_tow_dest).replace('incr_stage_payment',incrementaldate_payment).replace('incr_date_report',incrementaldate_reports).replace('incr_date_gpshist',incrementaldate_gpshist).replace('facil_id',incrementaldate_statgrp).replace('incr_date_adj',incrementaldate_adj).replace('incr_date_sc_call',incrementaldate_sc_call).replace('incr_date_session_stats',incrementaldate_session_stats).replace('incr_date_spot_grid',incrementaldate_spot_grid).replace('incrdate_tow_dest_list',incrementaldate_tow_dest_list).replace('incrdate_sc_call_comm',incrementaldate_sc_call_comm)
          logging.info('Query is %s',query)
          logging.info('Query submitted to Oracle Database..')
          
          if targettable == 'WORK_ERS_STG_SPOT_GRID':
              for chunk in pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=500):
                  chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['workdataset']+"."+targettable, env_config['projectid'],if_exists='append')
          else:
              for chunk in pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=200000):
                  chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['workdataset']+"."+targettable, env_config['projectid'],if_exists='append')
                         
          logging.info("Load completed...")
          return list("1")
      
class runbqsql(beam.DoFn):
      def process(self, context, inputquery,targettable=None):
          client = bigquery.Client(project=env_config['projectid'])
          query=inputquery
          table=targettable
          if table != None:
           job_config = bigquery.QueryJobConfig()
           if table == 'ERS_STAGE_CAD_SERVICE_FACILITY':
            table_ref = client.dataset('REFERENCE').table(table)
           else:
            table_ref = client.dataset(product_config['datasetname']).table(table)    
           job_config.destination=table_ref
          # table is not None 
           job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
         
           query_job = client.query(query,
               job_config=job_config)
           results = query_job.result()
           print(results)
           return list("1")   
          else:
           query_job = client.query(query)
           results = query_job.result()
           print(results)
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
                     '--service_account_key_file', env_config['service_account_key_file'],
                     '--worker_machine_type', "n1-standard-8",
                     '--extra_package', env_config['extra_package']
                     ]
    
    try:
        
        pcoll = beam.Pipeline(argv=pipeline_args)
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])
        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())

        (dummy_env | 'WORK D2K AUDIT' >>  beam.ParDo(readfromoracle(),landing_ers_d2k_audit,'WORK_ERS_STG_APD_D2K_AUDIT')  
        | 'D2K AUDIT' >>  beam.ParDo(runbqsql(),merge_ers_stage_d2k_audit)) 
  
        (dummy_env | 'WORK APD REMB DETAIL' >>  beam.ParDo(readfromoracle(),landing_ers_apd_remb_detail,'WORK_ERS_STG_APD_REMB_DETAIL')  
        | 'APD REMB DETAIL' >>  beam.ParDo(runbqsql(),merge_ers_stage_apd_remb_detail))    
        (dummy_env | 'WORK APD SVC FAC ADJ ITEM' >>  beam.ParDo(readfromoracle(),landing_ers_apd_svc_fac_adj_item,'WORK_ERS_STG_APD_SVC_FAC_ADJ_ITEM')  
        | 'APD SVC FAC ADJ ITEM' >>  beam.ParDo(runbqsql(),merge_ers_stage_apd_svc_fac_adj_item))  
           
        (dummy_env | 'WORK CAD SVC VEHICLE' >>  beam.ParDo(readfromoracle(),landing_ers_cad_svc_vehicle,'WORK_ERS_STG_CAD_SVC_VEHICLE')  
        | 'CAD SVC VEHICLE' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_svc_vehicle))  
         
        (dummy_env | 'WORK HIST AAR REFERRAL' >>  beam.ParDo(readfromoracle(),landing_ers_hist_aar_referral,'WORK_ERS_STG_HIST_AAR_REFERRAL')  
        | 'HIST AAR REFERRAL' >>  beam.ParDo(runbqsql(),merge_ers_stage_hist_aar_referral))  
            
        (dummy_env | 'WORK INQ ENTITLEMENT PERIODS' >>  beam.ParDo(readfromoracle(),landing_ers_inq_ent_periods,'WORK_ERS_STG_INQ_ENTITLEMENT_PERIODS')  
        | 'INQ ENTITLEMENT PERIODS' >>  beam.ParDo(runbqsql(),merge_ers_stage_inq_ent_periods))  
       
        (dummy_env | 'WORK CAD TOW DEST' >>  beam.ParDo(readfromoracle(),landing_ers_cad_tow_dest,'WORK_ERS_STG_CAD_TOW_DEST')  
        | 'CAD TOW DEST' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_tow_dest))  
      
        (dummy_env | 'WORK PAYMENT' >>  beam.ParDo(readfromoracle(),landing_ers_stage_payment,'WORK_ERS_STG_PAYMENT')  
        | 'PAYMENT' >>  beam.ParDo(runbqsql(),merge_ers_stage_payment))  
      
        (dummy_env | 'WORK REPORTS_DATA' >>  beam.ParDo(readfromoracle(),landing_ers_stage_reports_data,'WORK_ERS_STG_REPORTS_DATA')  
        | 'REPORTS_DATA' >>  beam.ParDo(runbqsql(),merge_ers_stage_reports_data))  
              
        (dummy_env | 'WORK STAT_GRP' >>  beam.ParDo(readfromoracle(),landing_ers_stage_stat_grp,'WORK_ERS_STG_STAT_GRP')  
        | 'STAT_GRP' >>  beam.ParDo(runbqsql(),merge_ers_stage_stat_grp))  
              
        (dummy_env | 'WORK TRUCK_GPS_HIST' >>  beam.ParDo(readfromoracle(),landing_ers_stage_truck_gps_hist,'WORK_ERS_STG_TRUCK_GPS_HIST')  
        | 'TRUCK_GPS_HIST' >>  beam.ParDo(runbqsql(),merge_ers_stage_truck_gps_hist))  
    
        (dummy_env | 'WORK APD REIMBURSEMENT' >>  beam.ParDo(readfromoracle(),landing_ers_apd_reimbursement,'WORK_ERS_STG_APD_REIMBURSEMENT')  
        | 'ERS APD REIMBURSEMENT' >>  beam.ParDo(runbqsql(),merge_ers_stage_apd_reimbursement))  
    
        (dummy_env | 'WORK HIST CALL ADJ' >>  beam.ParDo(readfromoracle(),landing_ers_hist_call_adj,'WORK_ERS_STG_HIST_CALL_ADJ')  
        | 'HIST CALL ADJ' >>  beam.ParDo(runbqsql(),merge_ers_stage_call_adj))  
    
        (dummy_env | 'WORK CAD TRUCK SESSION STATS' >>  beam.ParDo(readfromoracle(),landing_ers_cad_session_stats,'WORK_ERS_STG_CAD_TRUCK_SESSION_STATS')  
        | 'CAD TRUCK SESSION STATS' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_session_stats))  
    
        (dummy_env | 'WORK CAD SC CALL COMMENT' >>  beam.ParDo(readfromoracle(),landing_ers_cad_sc_call_comment,'WORK_ERS_STG_CAD_SC_CALL_COMMENT')  
        | 'CAD SC CALL COMMENT' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_sc_call_comment))  
            
        (dummy_env | 'WORK SPOT GRID' >>  beam.ParDo(readfromoracle(),landing_ers_spot_grid,'WORK_ERS_STG_SPOT_GRID')  
        | 'SPOT GRID' >>  beam.ParDo(runbqsql(),merge_ers_stage_spot_grid) | 'SERVICE FACILITY GRID' >>  beam.ParDo(runbqsql(),merge_ers_service_fac_grid))
        (dummy_env | 'WORK SC CALL COMMENT' >>  beam.ParDo(readfromoracle(),landing_ers_sc_call_comment,'WORK_ERS_STG_SC_CALL_COMMENT')         | 'SC CALL COMMENT' >>  beam.ParDo(runbqsql(),merge_ers_stage_sc_call_comment))                          (dummy_env | 'WORK CAD TOW DEST LIST' >>  beam.ParDo(readfromoracle(),landing_ers_tow_dest_list,'WORK_ERS_STG_TOW_DEST_LIST')          | 'CAD TOW DEST LIST' >>  beam.ParDo(runbqsql(),merge_ers_stage_tow_dest_list))                                                                     (dummy_env | 'WORK CDX FEEDBACK' >>  beam.ParDo(readfromoracle(),landing_ers_cdx_feedback,'WORK_ERS_STG_CDX_FEEDBACK')         | 'CDX FEEDBACK' >>  beam.ParDo(runbqsql(),merge_ers_stage_cdx_feedback,'ERS_STAGE_CDX_FEEDBACK'))
        p=pcoll.run()
        p.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise    

def main(args_config,args_productconfig,args_env,args_connectionprefix,args_incrementaldate):
    logging.getLogger().setLevel(logging.INFO)
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(utilpath+'readconfig.py').read(), utilpath+'readconfig.py', 'exec'), globals())
    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global connectionprefix
    connectionprefix = args_connectionprefix
    global incrementaldate
    incrementaldate=args_incrementaldate
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    global sqlstring
    jobname="load-incr-"+product_config['productname']+"-"+"landing"+"-"+time.strftime("%Y%m%d")
    logging.info('Job Name is %s',jobname)

    global landing_ers_d2k_audit
    global landing_ers_apd_remb_detail
    global landing_ers_apd_svc_fac_adj_item
    global landing_ers_cad_svc_vehicle    
    global landing_ers_hist_aar_referral    
    global landing_ers_inq_ent_periods    
    global landing_ers_cad_tow_dest    
    global landing_ers_spot_grid
    global landing_ers_stage_payment
    global landing_ers_stage_reports_data
    global landing_ers_stage_stat_grp
    global landing_ers_stage_truck_gps_hist       
    global landing_ers_apd_reimbursement
    global landing_ers_hist_call_adj
    global landing_ers_cad_session_stats
    global landing_ers_cad_sc_call_comment                  global landing_ers_tow_dest_list 
    global landing_ers_sc_call_comment                      global landing_ers_cdx_feedback
    landing_ers_d2k_audit = readfileasstring(cwd+'/sql/incremental/insert_d2k_audit.sql')
    landing_ers_apd_remb_detail = readfileasstring(cwd+'/sql/incremental/insert_apd_remb_detail.sql')   
    landing_ers_apd_svc_fac_adj_item = readfileasstring(cwd+'/sql/incremental/insert_apd_svc_fac_adj_item.sql')   
    landing_ers_cad_svc_vehicle = readfileasstring(cwd+'/sql/incremental/insert_cad_svc_vehicle.sql')   
    landing_ers_hist_aar_referral = readfileasstring(cwd+'/sql/incremental/insert_hist_aar_referral.sql')   
    landing_ers_inq_ent_periods = readfileasstring(cwd+'/sql/incremental/insert_inq_ent_periods.sql')   
    landing_ers_cad_tow_dest = readfileasstring(cwd+'/sql/incremental/insert_cad_tow_dest.sql')   
    landing_ers_spot_grid = readfileasstring(cwd+'/sql/incremental/insert_ers_spot_grid.sql')  
    landing_ers_stage_payment=readfileasstring(cwd+'/sql/incremental/insert_payments.sql')
    landing_ers_stage_reports_data =readfileasstring(cwd+'/sql/incremental/insert_reports_data.sql')
    landing_ers_stage_stat_grp =readfileasstring(cwd+'/sql/incremental/insert_stat_grp.sql')
    landing_ers_stage_truck_gps_hist =readfileasstring(cwd+'/sql/incremental/insert_truck_gps_hist.sql')
    landing_ers_apd_reimbursement = readfileasstring(cwd+'/sql/incremental/insert_apd_reimbursement.sql')
    landing_ers_hist_call_adj = readfileasstring(cwd+'/sql/incremental/insert_hist_call_adj.sql')
    landing_ers_cad_session_stats = readfileasstring(cwd+'/sql/incremental/insert_cad_session_stats.sql')
    landing_ers_cad_sc_call_comment = readfileasstring(cwd+'/sql/incremental/insert_cad_sc_call_comment.sql')
    landing_ers_tow_dest_list = readfileasstring(cwd+'/sql/incremental/insert_tow_dest_list.sql')       landing_ers_sc_call_comment = readfileasstring(cwd+'/sql/incremental/insert_sc_call_comment.sql')    landing_ers_cdx_feedback = readfileasstring(cwd+'/sql/incremental/insert_cdx_feedback.sql')
    global merge_ers_stage_d2k_audit
    global merge_ers_stage_apd_remb_detail
    global merge_ers_stage_apd_svc_fac_adj_item
    global merge_ers_stage_cad_svc_vehicle
    global merge_ers_stage_hist_aar_referral
    global merge_ers_stage_inq_ent_periods
    global merge_ers_stage_cad_tow_dest
    global merge_ers_stage_spot_grid
    global merge_ers_service_fac_grid
    global merge_ers_stage_payment
    global merge_ers_stage_reports_data
    global merge_ers_stage_stat_grp
    global merge_ers_stage_truck_gps_hist
    global merge_ers_stage_apd_reimbursement
    global merge_ers_stage_call_adj
    global merge_ers_stage_cad_session_stats
    global merge_ers_stage_cad_sc_call_comment 
    global merge_ers_stage_tow_dest_list    global merge_ers_stage_sc_call_comment         global merge_ers_stage_cdx_feedback
    merge_ers_stage_d2k_audit=readfileasstring(cwd+'/sql/incremental/merge_d2k_audit.sql')
    merge_ers_stage_apd_remb_detail=readfileasstring(cwd+'/sql/incremental/merge_apd_remb_detail.sql')
    merge_ers_stage_apd_svc_fac_adj_item=readfileasstring(cwd+'/sql/incremental/merge_apd_svc_fac_adj_item.sql')
    merge_ers_stage_cad_svc_vehicle=readfileasstring(cwd+'/sql/incremental/merge_cad_svc_vehicle.sql')
    merge_ers_stage_hist_aar_referral=readfileasstring(cwd+'/sql/incremental/merge_hist_aar_referral.sql')
    merge_ers_stage_inq_ent_periods=readfileasstring(cwd+'/sql/incremental/merge_inq_ent_periods.sql')
    merge_ers_stage_cad_tow_dest=readfileasstring(cwd+'/sql/incremental/merge_cad_tow_dest.sql')
    merge_ers_stage_spot_grid=readfileasstring(cwd+'/sql/incremental/merge_ers_spot_grid.sql')
    merge_ers_service_fac_grid=readfileasstring(cwd+'/sql/incremental/merge_ers_service_fac_grid.sql')    
    merge_ers_stage_payment=readfileasstring(cwd+'/sql/incremental/merge_payment.sql')
    merge_ers_stage_reports_data=readfileasstring(cwd+'/sql/incremental/merge_reports_data.sql')
    merge_ers_stage_stat_grp=readfileasstring(cwd+'/sql/incremental/merge_ers_stage_stat_grp.sql')
    merge_ers_stage_truck_gps_hist=readfileasstring(cwd+'/sql/incremental/merge_truck_gps_hist.sql')
    merge_ers_stage_apd_reimbursement=readfileasstring(cwd+'/sql/incremental/merge_apd_reimbursement.sql')
    merge_ers_stage_call_adj=readfileasstring(cwd+'/sql/incremental/merge_hist_call_adj.sql')
    merge_ers_stage_cad_session_stats=readfileasstring(cwd+'/sql/incremental/merge_cad_session_stats.sql')
    merge_ers_stage_cad_sc_call_comment=readfileasstring(cwd+'/sql/incremental/merge_cad_sc_call_comment.sql')
    merge_ers_stage_tow_dest_list=readfileasstring(cwd+'/sql/incremental/merge_tow_dest_list.sql')    merge_ers_stage_sc_call_comment=readfileasstring(cwd+'/sql/incremental/merge_sc_call_comment.sql')    merge_ers_stage_cdx_feedback = readfileasstring(cwd+'/sql/incremental/merge_cdx_feedback.sql')
    global incrementaldate_d2k    
    global incremental_svc_fac_itm
    global incrementaldate_cad_svc_veh    
    global incrementaldate_aar_referral    
    global incrementaldate_enti_period
    global incrementaldate_tow_dest
    global incrementaldate_payment
    global incrementaldate_reports
    global incrementaldate_gpshist
    global incrementaldate_statgrp    
    global incrementaldate_reim
    global incrementaldate_adj
    global incrementaldate_sc_call
    global incrementaldate_session_stats    
    global incrementaldate_spot_grid
    global incrementaldate_tow_dest_list    global incrementaldate_sc_call_comm    global incrementaldate_cdx_feedback
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
    
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket('dw-prod-ers')

    blob_d2k = Blob('paramfiles/result_D2K_DATETIME.csv', bucket)
    print("blob name is "+blob_d2k.name)
    downloaded_blob_d2k = blob_d2k.download_as_string()
    date_string_d2k =downloaded_blob_d2k[14:24]
    incrementaldate_d2k=date_string_d2k.decode("utf-8")            
    blob_cad_svc_veh = Blob('paramfiles/result_CAD_SVC_VEHICLE.csv', bucket)
    print("blob name is "+blob_cad_svc_veh.name)
    downloaded_blob_cad_svc_veh = blob_cad_svc_veh.download_as_string()
    date_string_cad_svc_veh =downloaded_blob_cad_svc_veh[14:24]
    incrementaldate_cad_svc_veh=date_string_cad_svc_veh.decode("utf-8")

    blob_aar_referral = Blob('paramfiles/result_AAR_REFERRAL.csv', bucket)
    print("blob name is "+blob_aar_referral.name)
    downloaded_blob_aar_referral = blob_aar_referral.download_as_string()
    date_string_aar_referral =downloaded_blob_aar_referral[14:24]
    incrementaldate_aar_referral=date_string_aar_referral.decode("utf-8")

    blob_enti_period = Blob('paramfiles/result_MIN_ENTI_DATETIME.csv', bucket)
    print("blob name is "+blob_enti_period.name)
    downloaded_blob_enti_period = blob_enti_period.download_as_string()
    date_string_enti_period =downloaded_blob_enti_period[14:24]
    incrementaldate_enti_period=date_string_enti_period.decode("utf-8")

    blob_tow_dest = Blob('paramfiles/result_TOW_DEST_DATETIME.csv', bucket)
    print("blob name is "+blob_tow_dest.name)
    downloaded_blob_tow_dest = blob_tow_dest.download_as_string()
    date_string_tow_dest =downloaded_blob_tow_dest[14:24]
    incrementaldate_tow_dest=date_string_tow_dest.decode("utf-8")

    blob_payment = Blob('paramfiles/result_PAY_DATETIME.csv', bucket)
    print("blob name is "+blob_payment.name)
    downloaded_blob_payment = blob_payment.download_as_string()
    date_string_payment =downloaded_blob_payment[14:33]
    incrementaldate_payment=date_string_payment.decode("utf-8")

    blob_reports = Blob('paramfiles/result_REPORTS_DATETIME.csv', bucket)
    print("blob name is "+blob_reports.name)
    downloaded_blob_reports = blob_reports.download_as_string()
    date_string_reports =downloaded_blob_reports[14:33]
    incrementaldate_reports=date_string_reports.decode("utf-8")

    blob_gpshist = Blob('paramfiles/result_GPSHIST_DATETIME.csv', bucket)
    print("blob name is "+blob_gpshist.name)
    downloaded_blob_gpshist = blob_gpshist.download_as_string()
    date_string_gpshist =downloaded_blob_gpshist[14:33]
#     print("date string is "+date_string_gpshist) 
    incrementaldate_gpshist =date_string_gpshist.decode("utf-8")
    
    blob_statgrp = Blob('paramfiles/result_FACILINT_DATETIME.csv', bucket)
    print("blob name is "+blob_statgrp.name)
    downloaded_blob_statgrp = blob_statgrp.download_as_string()
    date_string_statgrp =downloaded_blob_statgrp[14:33]
#     print("date string is "+date_string_statgrp)
    incrementaldate_statgrp =date_string_statgrp.decode("utf-8")
    
    blob_reim= Blob('paramfiles/result_MIN_REIMBURSEMENT_DATETIME.csv', bucket)
    print("blob name is "+blob_reim.name)
    downloaded_blob_reim = blob_reim.download_as_string()
    date_string_reim =downloaded_blob_reim[14:33]
    incrementaldate_reim =date_string_reim.decode("utf-8")
    
    blob_adj = Blob('paramfiles/result_CALL_ADJ_DATETIME.csv', bucket)
    print("blob name is "+blob_adj.name)
    downloaded_blob_adj = blob_adj.download_as_string()
    date_string_adj =downloaded_blob_adj[14:33]
#     print("date string is "+date_string_adj) 
    incrementaldate_adj =date_string_adj.decode("utf-8")

    blob_sc_call = Blob('paramfiles/result_CAD_SC_CALL_COMMENT.csv', bucket)
    print("blob name is "+blob_sc_call.name)
    downloaded_blob_sc_call = blob_sc_call.download_as_string()
    date_string_sc_call =downloaded_blob_sc_call[14:33]
#     print("date string is "+date_string_sc_call) 
    incrementaldate_sc_call =date_string_sc_call.decode("utf-8")
    
    blob_session_stats = Blob('paramfiles/result_SESSION_STATS.csv', bucket)
    print("blob name is "+blob_session_stats.name)
    downloaded_blob_session_stats = blob_session_stats.download_as_string()
    date_string_session_stats =downloaded_blob_session_stats[14:33]
#     print("date string is "+date_string_session_stats) 
    incrementaldate_session_stats =date_string_session_stats.decode("utf-8")   

    blob_spot_grid = Blob('paramfiles/result_SPOT_GRID.csv', bucket)
    print("blob name is "+blob_spot_grid.name)
    downloaded_blob_spot_grid = blob_spot_grid.download_as_string()
    date_string_spot_grid  =downloaded_blob_spot_grid [14:33]
#     print("date string is "+date_string_spot_grid)
    incrementaldate_spot_grid =date_string_spot_grid.decode("utf-8")
    blob_tow_dest_list = Blob('paramfiles/result_TOW_DEST_LIST.csv', bucket)     print("blob name is "+blob_tow_dest_list.name)     downloaded_blob_tow_dest_list = blob_tow_dest_list.download_as_string()     date_string_tow_dest_list =downloaded_blob_tow_dest_list[14:24]     incrementaldate_tow_dest_list=date_string_tow_dest_list.decode("utf-8")                       blob_sc_call_comm = Blob('paramfiles/result_SC_CALL_COMMENT.csv', bucket)     print("blob name is "+blob_sc_call_comm.name)     downloaded_blob_sc_call_comm = blob_sc_call_comm.download_as_string()     date_string_sc_call_comm =downloaded_blob_sc_call_comm[14:33] #     print("date string is "+date_string_sc_call_comm)      incrementaldate_sc_call_comm =date_string_sc_call_comm.decode("utf-8")                                                        
    print("incrementadldate_d2k is")
    print(incrementaldate_d2k)
    print("incrementaldate_cad_svc_veh is")
    print(incrementaldate_cad_svc_veh)      
    print("incrementaldate_aar_referral is")
    print(incrementaldate_aar_referral)    
    print("incrementadldate_payment is")
    print(incrementaldate_payment)
    print("incrementadldate_reports is")
    print(incrementaldate_reports)
    print("incrementadldate_gpshist is")
    print(incrementaldate_gpshist)
    print("incrementadldate_facilid is")
    print(incrementaldate_statgrp)
    print("incrementaldate_reim is")
    print(incrementaldate_reim)
    print("incrementaldate_adj is")
    print(incrementaldate_adj)
    print("incrementaldate_sc_call is")
    print(incrementaldate_sc_call)
    print("incrementaldate_session_stats is")
    print(incrementaldate_session_stats)      
    print("incrementaldate_spot_grid is")
    print(incrementaldate_spot_grid)
    print("incrementaldate_sc_call_comm is")     print(incrementaldate_sc_call_comm)             print("incrementaldate_tow_dest_list is")     print(incrementaldate_tow_dest_list)           
    run()  


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--config',
        required=True,
        help= ('Config file name')
        )
    parser.add_argument(
        '--productconfig',
        required=True,
        help= ('product Config file name')
        )
    parser.add_argument(
        '--env',
        required=True,
        help= ('Enviornment to be run dev/test/prod')
        )
      
    parser.add_argument(
        '--connectionprefix',
        required=True,
        help= ('connectionprefix either Schema')
        )
    
    parser.add_argument(
        '--incrementaldate',
        required=True,
        help= ('Incremental Data Pull Filter date')
        )

    args = parser.parse_args()   
        
    main(args.config,
         args.productconfig,
         args.env,
         args.connectionprefix,
         args.incrementaldate
         )    

