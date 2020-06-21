from time import gmtime, strftime
from google.cloud import bigquery
import argparse
import logging
from google.cloud import storage
from google.cloud.storage import Blob
from oauth2client.client import GoogleCredentials
import os

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

def df_from_bq(query,table=None,compute=False):    
 client = bigquery.Client.from_service_account_json(env_config["service_account_key_file"]) #Authentication if BQ using ServiceKey
 project = env_config['projectid']
 table_name = 'result_'+table

 job_config = bigquery.QueryJobConfig()
 table_ref = client.dataset(product_config['datasetname']).table(table_name)
 job_config.destination = table_ref
 job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #Creates the table with query result. Overwrites it if the table exists

 query_job = client.query(
    query,
    location='US', 
    job_config=job_config)
 query_job.result() 
 print(('Query results loaded to table {}'.format(table_ref.path)))

 destination_uri = "gs://dw-prod-ers/paramfiles/{}".format(table_name+'.csv') 
 dataset_ref = client.dataset(product_config['datasetname'], project=project)
 table_ref = dataset_ref.table(table_name)

 extract_job = client.extract_table(
    table_ref,
    destination_uri,
    location='US') 
 extract_job.result() #Extracts results to the GCS


 print(('Query results extracted to GCS: {}'.format(destination_uri)))

 client.delete_table(table_ref) #Deletes table in BQ

 print(('Table {} deleted'.format(table_name)))


 print('Results imported to DD!') 
 return "true"

def main(args_config,args_productconfig,args_env):
    logging.getLogger().setLevel(logging.INFO)
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config
    exec(compile(open(utilpath+"readconfig.py").read(), utilpath+"readconfig.py", 'exec'), globals()) 
    env_config =readconfig(config,env)
    global product_config
    product_config = readconfig(productconfig,env)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
    df_from_bq('SELECT max(TIMESTAMP_DT) as MAX_TMSP_DATE FROM `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_APD_D2K_AUDIT`','D2K_DATETIME') 
    df_from_bq('SELECT max(FACIL_INT_ID) as MAX_FACL_INID FROM `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_APD_SVC_FAC_ADJ_ITEM`','FACIL_INT_ID') 
    df_from_bq('SELECT max(LAST_UPDATE) as MAX_LAST_DATE FROM `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_CAD_SVC_VEHICLE`','CAD_SVC_VEHICLE') 
    df_from_bq('SELECT max(TD_SC_DT) as MAX_TDSC_DATE FROM `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_HIST_AAR_REFERRAL`','AAR_REFERRAL') 
    df_from_bq('SELECT MAX( ENT_START_DATE ) AS MAX_ENTI_DATE FROM OPERATIONAL.ERS_STAGE_INQ_ENTITLEMENT_PERIODS WHERE ENT_START_DATE<=CURRENT_DATE()','MIN_ENTI_DATETIME') 
    df_from_bq('SELECT max(LAST_UPDATE) as MAX_TOWD_DATE FROM `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_CAD_TOW_DEST`','TOW_DEST_DATETIME') 
    df_from_bq('Select max(PAY_DATETIME) as MAX_PAYM_DATE from OPERATIONAL.ERS_STAGE_PAYMENT','PAY_DATETIME') 
    df_from_bq('Select max(ARCH_DATETIME) as MAX_ARCH_DATE from OPERATIONAL.ERS_STAGE_REPORTS_DATA','REPORTS_DATETIME') 
    df_from_bq('Select max(UPDAT_DT) as MAX_UPDA_DATE from OPERATIONAL.ERS_STAGE_TRUCK_GPS_HIST','GPSHIST_DATETIME') 
    df_from_bq('SELECT max(Start_dtm) as MAX_FACI_DATE FROM OPERATIONAL.ERS_STAGE_STAT_GRP' ,'FACILINT_DATETIME') 
    df_from_bq('SELECT MIN(DT_MAX) AS MAX_REIM_DATE FROM (SELECT MAX(RECEIVED_DT) AS DT_MAX FROM OPERATIONAL.ERS_STAGE_APD_REIMBURSEMENT UNION ALL SELECT MAX(FOLLOWUP_DT) AS DT_MAX FROM OPERATIONAL.ERS_STAGE_APD_REIMBURSEMENT UNION ALL SELECT MAX(APPROVED_DT) AS DT_MAX FROM OPERATIONAL.ERS_STAGE_APD_REIMBURSEMENT UNION ALL SELECT MAX(PAY_DT) AS DT_MAX FROM OPERATIONAL.ERS_STAGE_APD_REIMBURSEMENT )' ,'MIN_REIMBURSEMENT_DATETIME') 
    df_from_bq('SELECT MAX(ADJ_RECORD_DT) AS MAX_CALL_DATE FROM OPERATIONAL.ERS_STAGE_HIST_CALL_ADJ' ,'CALL_ADJ_DATETIME') 
    df_from_bq('SELECT MAX(SC_COMM_ADD_TM) AS MAX_COMM_DATE FROM OPERATIONAL.ERS_STAGE_CAD_SC_CALL_COMMENT' ,'CAD_SC_CALL_COMMENT') 
    df_from_bq('SELECT MAX(INSERT_TM) AS MAX_STAT_DATE FROM OPERATIONAL.ERS_STAGE_CAD_TRUCK_SESSION_STATS' ,'SESSION_STATS') 
#     var_veh_data=df_from_bq('SELECT max(VEHICLE_ID) as MAX_VEHI_INID FROM `aaa-mwg-dwprod.OPERATIONAL.ERS_STAGE_CAD_VEHICLE_DATA`','VEHICLE_ID') 
    df_from_bq('SELECT MAX(LAST_MODIFIED) AS MAX_SPOT_DATE FROM OPERATIONAL.ERS_STAGE_SPOT_GRID' ,'SPOT_GRID')
     df_from_bq('SELECT MAX(LAST_UPDATE) AS MAX_LIST_DATE FROM OPERATIONAL.ERS_STAGE_TOW_DEST_LIST' ,'TOW_DEST_LIST')             df_from_bq('SELECT MAX(SC_COMM_ADD_TM) AS MAX_CALL_DATE FROM OPERATIONAL.ERS_STAGE_SC_CALL_COMMENT' ,'SC_CALL_COMMENT')
    client = storage.Client(env_config['projectid'])
    bucket = client.bucket('dw-prod-ers')

    blob_d2k = Blob('paramfiles/result_D2K_DATETIME.csv', bucket)
    print("blob name is "+blob_d2k.name)
    downloaded_blob_d2k = blob_d2k.download_as_string()
    date_string_d2k =downloaded_blob_d2k[14:24]
#     print("date string is "+date_string_d2k) 
    incrementaldate_d2k =date_string_d2k.decode("utf-8") 

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
    
    blob_apd_reim = Blob('paramfiles/result_MIN_REIMBURSEMENT_DATETIME.csv', bucket)
    print("blob name is "+blob_apd_reim.name)
    downloaded_blob_apd_reim = blob_apd_reim.download_as_string()
    date_string_apd_reim =downloaded_blob_apd_reim[14:33]
#     print("date string is "+date_string_apd_reim)
    incrementaldate_apd_reim =date_string_apd_reim.decode("utf-8")
    
    blob_call_adj = Blob('paramfiles/result_CALL_ADJ_DATETIME.csv', bucket)
    print("blob name is "+blob_call_adj.name)
    downloaded_blob_call_adj = blob_call_adj.download_as_string()
    date_string_call_adj =downloaded_blob_call_adj[14:33]
#     print("date string is "+date_string_call_adj)
    incrementaldate_call_adj =date_string_call_adj.decode("utf-8")

    blob_sc_call_comment = Blob('paramfiles/result_CAD_SC_CALL_COMMENT.csv', bucket)
    print("blob name is "+blob_sc_call_comment.name)
    downloaded_blob_sc_call_comment = blob_sc_call_comment.download_as_string()
    date_string_sc_call_comment  =downloaded_blob_sc_call_comment [14:33]
#     print("date string is "+date_string_sc_call_comment)
    incrementaldate_sc_call_comment =date_string_sc_call_comment.decode("utf-8")

    blob_session_stats = Blob('paramfiles/result_SESSION_STATS.csv', bucket)
    print("blob name is "+blob_session_stats.name)
    downloaded_blob_session_stats = blob_session_stats.download_as_string()
    date_string_session_stats  =downloaded_blob_session_stats [14:33]
#     print("date string is "+date_string_session_stats)
    incrementaldate_session_stats =date_string_session_stats.decode("utf-8")   

    blob_spot_grid = Blob('paramfiles/result_SPOT_GRID.csv', bucket)
    print("blob name is "+blob_spot_grid.name)
    downloaded_blob_spot_grid = blob_spot_grid.download_as_string()
    date_string_spot_grid  =downloaded_blob_spot_grid [14:33]
#     print("date string is "+date_string_spot_grid)
    incrementaldate_spot_grid =date_string_spot_grid.decode("utf-8")          
    blob_tow_dest_list = Blob('paramfiles/result_TOW_DEST_LIST.csv', bucket)    print("blob name is "+blob_tow_dest_list.name)    downloaded_blob_tow_dest_list = blob_tow_dest_list.download_as_string()    date_string_tow_dest_list =downloaded_blob_tow_dest_list[14:24]    incrementaldate_tow_dest_list=date_string_tow_dest_list.decode("utf-8")            blob_sc_call_comm = Blob('paramfiles/result_SC_CALL_COMMENT.csv', bucket)    print("blob name is "+blob_sc_call_comm.name)    downloaded_blob_sc_call_comm = blob_sc_call_comm.download_as_string()    date_string_sc_call_comm  =downloaded_blob_sc_call_comm [14:33]#     print("date string is "+date_string_sc_call_comm)    incrementaldate_sc_call_comm =date_string_sc_call_comm.decode("utf-8")                                   
    print("Incremnetaldate_d2k is")
    print(incrementaldate_d2k)    
    print("incrementaldate_cad_svc_veh is")
    print(incrementaldate_cad_svc_veh)
    print("incrementaldate_aar_referral is")
    print(incrementaldate_aar_referral)
    print("incrementaldate_enti_period is")
    print(incrementaldate_enti_period)
    print("incrementaldate_tow_dest is")
    print(incrementaldate_tow_dest)
    print("incrementadldate_payment is")
    print(incrementaldate_payment)
    print("incrementadldate_reports is")
    print(incrementaldate_reports)
    print("incrementadldate_gpshist is")
    print(incrementaldate_gpshist)
    print("incrementadldate_facilid is")
    print(incrementaldate_statgrp)
    print("incrementadldate_apd_reim is")
    print(incrementaldate_apd_reim)
    print("incrementaldate_call_adj is")
    print(incrementaldate_call_adj)
    print("incrementaldate_sc_call_comment is")
    print(incrementaldate_sc_call_comment)
    print("incrementaldate_session_stats is")
    print(incrementaldate_session_stats)
    print("incrementaldate_spot_grid is")
    print(incrementaldate_spot_grid)
    print("incrementaldate_tow_dest_list is")    print(incrementaldate_tow_dest_list)          print("incrementaldate_sc_call_comm is")    print(incrementaldate_sc_call_comm)           
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
    
    args = parser.parse_args()       
 
main(args.config,
     args.productconfig,
     args.env)       
