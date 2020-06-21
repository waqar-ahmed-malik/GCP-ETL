import datetime
import os
import argparse
import csv
from google.cloud import bigquery
import logging
import sys
import pymssql
#import pyodbc
sys.path.append(os.path.abspath('..\deployment'))
from google.cloud import storage as gstorage
from google.cloud.storage import blob as Blob
from googleapiclient import discovery
import pandas
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

def load_csv_to_bigquery_using_bqsdk(projectid,
                datasetname,
                csv_file_path,
                outputTableName,
                fieldDelimiter=",",
                skipLeadingRows=1,
                writeDeposition='WRITE_TRUNCATE'):
        """This function will read data from csv and load in BigQuery destination table using BigQuery sdk insert job."""
        logging.info('Loading table '+outputTableName)
        print('Loading table {}'.format(outputTableName))
        
        try:
            bigqclient = bigquery.Client(project=projectid)
            tdatasetname = bigqclient.dataset(datasetname)
            table_ref = tdatasetname.table(outputTableName)
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
                                'maxBadRecords':0,
                                'allowJaggedRows':False,
                                'writeDisposition':'WRITE_TRUNCATE',
                                'sourceUris':[csv_file_path],
                                'fieldDelimiter':fieldDelimiter,
                                'allowQuotedNewlines':False,
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
    "run query, generate csv and loads in targettable"
    try:
        database_user=env_config[connectionprefix+'_database_user']
        database_password=env_config[connectionprefix+'_database_password']
        database_host=env_config[connectionprefix+'_database_host']
        database_database=env_config[connectionprefix+'_database']
        
        cnx = pymssql.connect(server=database_host, user=database_user,password=database_password,database=database_database)
        cnx.cursor()
        #query = loaddaily.readfileasstring(mysqlquery)
        query= "SELECT top(150000) mbr.mbr_ky, mbr.mbrs_id, null As CUSTOMER_KEY,mbr.mbr_assoc_id, mbr.mbr_jn_aaa_dt,mbr.mbr_jn_clb_dt, mbr.mbr_brth_dt,  mbr.mbr_dup_crd_ct,mbr.mbr_mid_init_nm, mbr.mbr_lst_nm, mbr.mbr_fst_nm, mbr.mbr_sts_dsc_cd, mbr.mbr_sts_cd, mbr.mbr_sltn_cd, mbr.mbr_relat_cd,mbr.mbr_rsn_join_cd, mbr.mbr_prev_club_cd,  mbr.mbr_prev_mbrs_id, mbr.mbr_do_not_ren_in, mbr.mbr_slct_cd,  mbr.mbr_src_sls_cd, mbr.mbr_canc_dt,mbr.mbr_comm_cd, 'null' AS COMPENSATE_EMPLOYEE_ID,mbr.mbrs_ky, mbr.mbr_typ_cd,mbr.mbr_paid_by_cd,mbr.mbr_free_assoc_in,mbr.mbr_dupcd_req_dt, 'null' AS SECONDARY_COMPENSATE_EMPLOYEE_ID,mbr.mbr_dup_stck_ct,mbr.mbr_dupstck_req_dt, mbr.mbr_bil_cat_cd, mbr.mbr_actv_dt,mbr.mbr_ers_usage_yr1, mbr.mbr_ers_usage_yr2, mbr.mbr_ers_usage_yr3,mbr.mbr_prev_sts_dsc_cd,'null' AS REINSTATEMENT_CARD_PRINT_INDICATOR, mbr.dup_card_reason, mbr.mbr_do_not_ren_rsn,mbr.mbr_card_name, mbr.mbr_language, mbr.card_reqt_skip_dt, mbr.mbr_eff_dt, mbr.mbr_prev_comm_cd, mbr.mbr_dnr_expir_dt, mbr.mbr_mita_pending,mbr.mbr_add_dt,  mbr.mbr_renew_ct, mbr.mbr_reinst_dt, mbr.card_reqt_skip_reason,mbr.mbr_expir_dt, mbr.pref_meth_cnt AS PREF_METH_COUNT,R.ride_comp_cd AS MEMBERSHIP_LEVEL, ISNULL(mbr.mbr_reinst_dt, mbr.mbr_jn_clb_dt) AS TENURE_DT,case when sp.SPCL_HNDL_CD= 'BM' then 'Y' else 'N' end as IS_BOARD_MEMBER, case when sp.SPCL_HNDL_CD= 'SPRINT' then sp.SPCL_HNDL_CMT_TX  end as SPRN_IND, null AS SPRINT_ENROLL_DT,case when sp.SPCL_HNDL_CD= 'HM' then 'Y' else 'N' end as HONORARY_MEMBER_IND,case when sp.SPCL_HNDL_CD= 'CM' then 'Y' else 'N' end as COURTESY_MEMBER_IND, case when sp.SPCL_HNDL_CD= 'UM' then 'Y' else 'N' end as UTAH_50_YEAR_IND, case when sp.SPCL_HNDL_CD IN ('SPP','SPA') then 'Y' else 'N' end as SERVICE_PROVIDER_IND,'null' AS SALES_CHANNEL ,DATEPART(YEAR,mbr.mbr_jn_aaa_dt) AS MEMBER_SINCE,case when sp.SPCL_HNDL_CD= 'NVIA' then 'Y' else 'N' end as VIA_MAGAZINE_PREFERRED_IND ,'null' AS CORPORATE_GROUP_TYPE ,null AS ETL_JOB_RUN_ID, 'null' AS SOURCE_SYSTEM_CD , null AS CREATE_DT, null AS UPDATE_DT , 'null' AS CREATE_BY FROM mbr left outer join sales_agent sa on mbr.sls_agt_ky = sa.sagt_ky left outer join sales_agent sec_sa on mbr.MBR_SEC_SAGT_KY = sec_sa.sagt_ky LEFT OUTER JOIN (SELECT * FROM ( SELECT *,DENSE_RANK() OVER(PARTITION BY MBRS_KY ORDER BY case  when ride_comp_cd= 'BS' THEN 1 WHEN ride_comp_cd= 'BS' THEN 1 WHEN ride_comp_cd= 'PL' THEN 2 WHEN ride_comp_cd= 'RP' THEN 3 WHEN ride_comp_cd= 'MO' THEN 4 WHEN ride_comp_cd= 'EP' THEN 5 WHEN ride_comp_cd= 'EV' THEN 5 END DESC) RN FROM RIDER WHERE ride_CANC_DT IS NULL AND (RIDE_EFF_DT IS NOT NULL OR RIDE_COMM_CD in ('T1', 'T')) AND UPPER(ride_STS_CD) <> 'C' ) RDR WHERE RDR.RN=1 ) R On R.mbrs_ky=mbr.mbrs_ky LEFT OUTER JOIN  (SELECT * FROM  (SELECT *, row_number() over (partition by mbr_ky,mbrs_ky ORDER BY spcl_ky desc) rn from  special_handling) sp where sp.rn=1) sp ON sp.mbr_ky=mbr.mbr_ky AND sp.mbrs_ky=mbr.mbrs_ky INNER JOIN (SELECT mbrs_ky FROM mbrship WHERE mbrship.mbrs_rtd2k_updt_dt > CAST('1900-01-01' as datetime) UNION select sq.mbrs_ky AS mbrs_ky  from  (SELECT distinct mbrship_updates.mbrs_ky AS mbrs_ky, mbr.mbr_ky AS mbr_ky, row_number() over (partition by mbrship_updates.mbrs_ky order by mbrship_updates.last_updt_dt desc) as rn FROM mbr, mbrship_updates WHERE mbrship_updates.mbrs_ky=mbr.mbrs_ky AND mbrship_updates.last_updt_dt > CAST('1900-01-01' as datetime) ) sq where sq.rn=1 UNION select sq.mbrs_ky AS mbrs_ky from  (SELECT distinct mbrship_comment.mbrs_ky AS mbrs_ky, mbr.mbr_ky AS mbr_ky, row_number() over (partition by mbr.mbr_ky order by mbrship_comment.mcmt_updt_dt desc) as rn FROM mbr, mbrship_comment WHERE mbrship_comment.mbrs_ky=mbr.mbrs_ky AND mbrship_comment.mcmt_updt_dt > CAST('1900-01-01' as datetime) ) sq where sq.rn=1 ) cdc ON cdc.mbrs_ky=mbr.mbrs_ky"
        print("Using following SQL to get results")
        print(query)
        print("\n refresh started for product: " + product_config['datasetname'])
        if not os.path.exists('data'):
            os.makedirs('data')
        source_csv_filename=product_config['datasetname']+targettable+'.csv'
        source_csv_filepath='data/'+source_csv_filename
        print(source_csv_filepath)
        queryresult=pandas.read_sql(query, cnx, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize= 100000)
        
        try:
            os.remove(source_csv_filepath)
        except OSError:
            pass
        
        f=open(source_csv_filepath, 'wb')
        
        for chunk in queryresult:  
         chunk.to_csv(f,encoding='utf-8',float_format='%.10g', header=False, quoting=1,index=False  , mode='a')    
        f.close()
        cnx.close()
                    
        print("Generated CSV File => data/{}.csv".format(source_csv_filename))
    
        exec(compile(open(os.path.abspath("././loadcsvtobigqueryusingbqsdk.py")).read(), os.path.abspath("././loadcsvtobigqueryusingbqsdk.py"), 'exec'), globals())

        projectname=env_config['projectid']
        storageclient = gstorage.Client(project=projectname)
        bucket = storageclient.get_bucket(product_config['bucket'])
        print(bucket)
        "chunk_size: The size of a chunk of data whenever iterating (1 MB). This must be a multiple of 256 KB per the API specification."
        blob = bucket.blob(source_csv_filename , chunk_size = 104857600)
        print(blob)
        blob.upload_from_filename(os.path.abspath("data")+"/"+source_csv_filename)

        gs_source_csv_filepath="gs://"+product_config['bucket']+"/"+source_csv_filename
     
        load_csv_to_bigquery_using_bqsdk(projectname,
                product_config['datasetname'],
                gs_source_csv_filepath,
                targettable,
                fieldDelimiter=",",
                skipLeadingRows=0,
                writeDeposition='WRITE_TRUNCATE')
        
        print("\nrefresh ended for product: " + product_config['datasetname'])
    except:
        logging.exception('Unable to fetch data from MySQL and create csv file and unable to load data into table ')
        raise
    
def main(args_config,args_productconfig,args_env,args_targettable,args_connectionprefix):
    global env
    env = args_env
    global config
    config= args_config
    global productconfig
    productconfig = args_productconfig
    global env_config

    exec(compile(open(os.path.abspath("././readconfig.py")).read(), os.path.abspath("././readconfig.py"), 'exec'), globals())

    env_config =readconfig(config, env)
    global product_config
    product_config = readconfig(productconfig,env)
    global targettable
    targettable = args_targettable
    global connectionprefix
    connectionprefix = args_connectionprefix
    
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
        '--targettable',
        required=True,
        help= ('targettable to which data would be loaded')
        )
    """
    parser.add_argument(
        '--mysqlquery',
        required=True,
        help= ('mysqlquery from which data needs to be extracted')
        )
    """     
    parser.add_argument(
        '--connectionprefix',
        required=True,
        help= ('connectionprefix either baas or caas')
        )

    args = parser.parse_args()   
    
    main(args.config,
         args.productconfig,
         args.env,
         args.targettable,
         args.connectionprefix)

