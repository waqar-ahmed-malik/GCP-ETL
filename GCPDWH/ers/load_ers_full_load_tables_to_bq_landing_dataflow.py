'''Created on Jun 26, 2019@author: Atul GuleriaThis module reads full ERS data from Oracle and Write into Bigquery Table. Truncate load is the method used.It uses pandas dataframe to read and write data to BigQuery.''' import apache_beam as beamimport timeimport jaydebeapi import base64import osimport argparsefrom google.cloud import bigqueryimport loggingimport sysfrom google.cloud import storagefrom google.cloud.storage import Blobfrom datetime import datetimeimport pandas as  pdfrom oauth2client.client import GoogleCredentialsprint((logging.__file__))cwd = os.path.dirname(os.path.abspath(__file__))if cwd.find("\\") > 0:    folderup = cwd.rfind("\\")    utilpath = cwd[:folderup]+"\\util\\"    #Else go with linux pathelse:    folderup = cwd.rfind("/")    utilpath = cwd[:folderup]+"/util/"def readfileasstring(sqlfile):       """    Read any text file and return text as a string.     This function is used to read .sql and .schema files    """     with open (sqlfile, "r") as file:         sqltext=file.read().strip("\n").strip("\r")    return sqltextclass setenv(beam.DoFn):       def process(self,context):          os.system('gsutil cp '+product_config['stagingbucket']+'/ojdbc6.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )          logging.info('Jar copied to Instance..')          logging.info('Java Libraries copied to Instance..')          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')          logging.info('Enviornment Variable set.')          return list("1")          class readfromoracle(beam.DoFn):       def process(self, context, inpquery,targettable):          database_user=env_config[connectionprefix+'_database_user']#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')          database_password = env_config[connectionprefix + '_database_password']          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')          database_host=env_config[connectionprefix+'_database_host']          database_port=env_config[connectionprefix+'_database_port']          database_db=env_config[connectionprefix+'_database']                    jclassname = "oracle.jdbc.driver.OracleDriver"          url = ("jdbc:oracle:thin:"+database_user+"/"+database_password+"@"+database_host +":"+database_port+"/"+database_db)          jars = ["/tmp/ojdbc6.jar"]          libs = None          cnx = jaydebeapi.connect(jclassname, url, jars=jars,                            libs=libs)             logging.info('Connection Successful..')           cnx.cursor()          logging.info('Reading Sql Query from the file...')                    query = inpquery.replace('jobrunid',str(jobrunid)).replace('jobname', jobname)          logging.info('Query is %s',query)          logging.info('Query submitted to Oracle Database..')           for chunk in pd.read_sql(query, cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=200000):                 chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['workdataset']+"."+targettable, env_config['projectid'],if_exists='replace')              logging.info("Load completed...")          return list("1")      class runbqsql(beam.DoFn):      def process(self, context, inputquery,targettable=None):          client = bigquery.Client(project=env_config['projectid'])          query=inputquery          table=targettable          if table != None:           job_config = bigquery.QueryJobConfig()           if table == 'ERS_STAGE_HIST_DUP_CALL_LST' :            table_ref = client.dataset(product_config['datasetname']).table(table)           else:            table_ref = client.dataset('REFERENCE').table(table)               job_config.destination=table_ref           job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE                    query_job = client.query(query,               job_config=job_config)           query_job.result()           return list("1")             else:           query_job = client.query(query)           query_job.result()           return list("1")            def run():    """    1. Set Dataflow PipeLine configurations.    2. Create PCollection element for each line read from the delimited file.    3. Tag values by calling RowValidator method i.e. clean records or broken records.    4. Call rowextractor method for cleaned records.    5. Write valid/clean records to BigQuery table mentioned in the parameter.    6. Sink the error records to error handler.      """        pipeline_args = ['--project', env_config['projectid'],                     '--job_name', jobname,                     '--runner', env_config['runner'],                     '--staging_location', product_config['stagingbucket'],                     '--temp_location', product_config['tempbucket'],                     '--requirements_file', env_config['requirements_file'],                     '--region', env_config['region'],                     '--zone',env_config['zone'],                     '--network',env_config['network'],                     '--subnetwork',env_config['subnetwork'],                     '--save_main_session', 'True',                     '--num_workers', env_config['num_workers'],                     '--max_num_workers', env_config['max_num_workers'],                     '--autoscaling_algorithm', env_config['autoscaling_algorithm'],                     '--service_account_name', env_config['service_account_name'],                     '--service_account_key_file', env_config['service_account_key_file'],                     '--worker_machine_type', "n1-standard-8",                     ]        try:                pcoll = beam.Pipeline(argv=pipeline_args)        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())        (dummy_env | 'WORK TOW DEST LIST ' >>  beam.ParDo(readfromoracle(),landing_ers_tow_dest_list,'WORK_ERS_TOW_DEST_LIST')        | 'TOW DEST LIST' >>  beam.ParDo(runbqsql(),merge_ers_tow_dest_list))        (dummy_env | 'WORK CAD EMPLOYEE' >>  beam.ParDo(readfromoracle(),landing_ers_cad_employee,'WORK_ERS_STG_CAD_EMPLOYEE')        | 'CAD EMPLOYEE' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_employee,'ERS_STAGE_CAD_EMPLOYEE'))        (dummy_env | 'WORK CAD CLUB INFO' >>  beam.ParDo(readfromoracle(),landing_ers_cad_club_info,'WORK_ERS_STG_CAD_CLUB_INFO')        | 'CAD CLUB INFO' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_club_info,'ERS_STAGE_CAD_CLUB_INFO'))        (dummy_env | 'WORK SERVICE_FACILITY' >>  beam.ParDo(readfromoracle(),landing_ers_stage_service_facility,'WORK_ERS_STG_SERVICE_FACILITY')        | 'SERVICE_FACILITY' >>  beam.ParDo(runbqsql(),merge_ers_stage_service_facility,'ERS_STAGE_SERVICE_FACILITY'))        (dummy_env | 'WORK SERVICE_TRUCK' >>  beam.ParDo(readfromoracle(),landing_ers_stage_service_truck,'WORK_ERS_STG_SERVICE_TRUCK')        | 'SERVICE_TRUCK' >>  beam.ParDo(runbqsql(),merge_ers_stage_service_truck,'ERS_STAGE_SERVICE_TRUCK'))        (dummy_env | 'WORK SVC_FAC_DBA' >>  beam.ParDo(readfromoracle(),landing_ers_stage_svc_fac_dba,'WORK_ERS_STG_SVC_FAC_DBA')        | 'SVC_FAC_DBA' >>  beam.ParDo(runbqsql(),merge_ers_stage_svc_fac_dba,'ERS_STAGE_SVC_FAC_DBA'))        (dummy_env | 'WORK CAD SERVICE FACILITY' >>  beam.ParDo(readfromoracle(),landing_ers_cad_service_fac,'WORK_ERS_STG_CAD_SERVICE_FACILITY')        | 'CAD SERVICE FACILITY' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_service_facility,'ERS_STAGE_CAD_SERVICE_FACILITY'))        (dummy_env | 'WORK HIST DUP CALL LST' >>  beam.ParDo(readfromoracle(),landing_ers_dup_call_lst,'WORK_ERS_STG_HIST_DUP_CALL_LST')        | 'HIST DUP CALL LST' >>  beam.ParDo(runbqsql(),merge_ers_stage_dup_call_lst,'ERS_STAGE_HIST_DUP_CALL_LST'))        (dummy_env | 'WORK LOOK UP' >>  beam.ParDo(readfromoracle(),landing_ers_work_look_up,'WORK_ERS_LOOK_UP')        | 'LOOK UP' >>  beam.ParDo(runbqsql(),merge_ers_look_up))        (dummy_env | 'WORK LOOK UP NAMES' >>  beam.ParDo(readfromoracle(),landing_ers_work_look_up_names,'WORK_ERS_LOOK_UP_NAMES')        | 'LOOK UP NAMES' >>  beam.ParDo(runbqsql(),merge_ers_look_up_names))        (dummy_env | 'WORK SVC TRK DRIVER' >>  beam.ParDo(readfromoracle(),landing_ers_work_svc_trk_driver,'WORK_ERS_SVC_TRK_DRIVER')        | 'SVC TRK DRIVER' >>  beam.ParDo(runbqsql(),merge_ers_svc_trk_driver))        (dummy_env | 'WORK PREDICTIVE TRUCK ASSIGNMENT' >>  beam.ParDo(readfromoracle(),landing_ers_work_predictive_truck_assignment,'WORK_ERS_PREDICTIVE_TRUCK_ASSIGNMENT')        | 'PREDICTIVE TRUCK ASSIGNMENT' >>  beam.ParDo(runbqsql(),merge_ers_predictive_truck_assignment))                        (dummy_env | 'WORK CAD VEHICLE DATA' >>  beam.ParDo(readfromoracle(),landing_ers_cad_vehicle_data,'WORK_ERS_STG_CAD_VEHICLE_DATA')        | 'CAD VEHICLE DATA' >>  beam.ParDo(runbqsql(),merge_ers_stage_cad_vehicle_data,'ERS_STAGE_CAD_VEHICLE_DATA'))        (dummy_env | 'WORK SVC TRK DRIVER CAP' >>  beam.ParDo(readfromoracle(),landing_ers_trk_drvier_cap,'WORK_ERS_STG_SVC_TRK_DRIVER_CAP')        | 'SVC TRK DRIVER CAP' >>  beam.ParDo(runbqsql(),merge_ers_stage_trk_drvier_cap,'ERS_STAGE_SVC_TRK_DRIVER_CAP'))                                                                                                                                      p=pcoll.run()        p.wait_until_finish()    except:        logging.exception('Failed to launch datapipeline')        raise    def main(args_config,args_productconfig,args_env,args_connectionprefix):    logging.getLogger().setLevel(logging.INFO)    global env    env = args_env    global config    config= args_config    global productconfig    productconfig = args_productconfig    global env_config    exec(compile(open(utilpath+'readconfig.py').read(), utilpath+'readconfig.py', 'exec'), globals())    env_config =readconfig(config, env)    global product_config    product_config = readconfig(productconfig,env)    global connectionprefix    connectionprefix = args_connectionprefix    global jobrunid    jobrunid=os.getpid()    logging.info('Job Run Id is %d',jobrunid)    global jobname    global sqlstring    jobname="load-full-load2-"+product_config['productname']+"-"+"landing"+"-"+time.strftime("%Y%m%d")    logging.info('Job Name is %s',jobname)    global landing_ers_cad_employee            global landing_ers_cad_club_info        global landing_ers_stage_service_facility    global landing_ers_stage_service_truck    global landing_ers_stage_svc_fac_dba    global landing_ers_cad_service_fac    global landing_ers_dup_call_lst           global landing_ers_tow_dest_list    global landing_ers_work_look_up    global landing_ers_work_look_up_names                   global landing_ers_work_svc_trk_driver    global landing_ers_work_predictive_truck_assignment    global landing_ers_cad_vehicle_data    global landing_ers_trk_drvier_cap          landing_ers_cad_employee = readfileasstring(cwd+'/sql/full_load/insert_cad_employee.sql')       landing_ers_cad_club_info = readfileasstring(cwd+'/sql/full_load/insert_cad_club_info.sql')       landing_ers_stage_service_facility =readfileasstring(cwd+'/sql/full_load/insert_service_facility.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)    landing_ers_stage_service_truck =readfileasstring(cwd+'/sql/full_load/insert_service_truck.sql')    landing_ers_stage_svc_fac_dba =readfileasstring(cwd+'/sql/full_load/insert_svc_fac_dba.sql')    landing_ers_cad_service_fac = readfileasstring(cwd+'/sql/full_load/insert_cad_service_fac.sql')    landing_ers_dup_call_lst = readfileasstring(cwd+'/sql/full_load/insert_dup_call_lst.sql')    landing_ers_tow_dest_list = readfileasstring(cwd+'/sql/full_load/work_ers_tow_dest_list.sql')    landing_ers_work_look_up =readfileasstring(cwd+'/sql/full_load/work_look_up.sql')    landing_ers_work_look_up_names =readfileasstring(cwd+'/sql/full_load/work_look_up_names.sql')    landing_ers_work_svc_trk_driver =readfileasstring(cwd+'/sql/full_load/work_ers_svc_trk_driver.sql')    landing_ers_work_predictive_truck_assignment =readfileasstring(cwd+'/sql/full_load/work_ers_predictive_truck_assignment.sql')    landing_ers_cad_vehicle_data = readfileasstring(cwd+'/sql/full_load/insert_cad_vehicle_data.sql')       landing_ers_trk_drvier_cap = readfileasstring(cwd+'/sql/full_load/insert_trk_driver_cap.sql')         global merge_ers_stage_cad_employee    global merge_ers_stage_cad_club_info    global merge_ers_stage_service_facility    global merge_ers_stage_service_truck    global merge_ers_stage_svc_fac_dba    global merge_ers_stage_cad_service_facility        global merge_ers_stage_dup_call_lst    global merge_ers_tow_dest_list    global merge_ers_look_up    global merge_ers_look_up_names    global merge_ers_svc_trk_driver    global merge_ers_predictive_truck_assignment    global merge_ers_stage_cad_vehicle_data    global merge_ers_stage_trk_drvier_cap            merge_ers_stage_cad_employee=readfileasstring(cwd+'/sql/full_load/merge_cad_employee.sql')    merge_ers_stage_cad_club_info=readfileasstring(cwd+'/sql/full_load/merge_cad_club_info.sql')    merge_ers_stage_service_facility=readfileasstring(cwd+'/sql/full_load/merge_service_facility.sql')    merge_ers_stage_service_truck=readfileasstring(cwd+'/sql/full_load/merge_service_truck.sql')    merge_ers_stage_svc_fac_dba=readfileasstring(cwd+'/sql/full_load/merge_ers_stage_svc_fac_dba.sql')    merge_ers_stage_cad_service_facility=readfileasstring(cwd+'/sql/full_load/merge_cad_service_facility.sql')    merge_ers_stage_dup_call_lst=readfileasstring(cwd+'/sql/full_load/merge_dup_call_lst.sql')    merge_ers_tow_dest_list=readfileasstring(cwd+'/sql/full_load/ers_aaa_auto_approved_repair.sql')    merge_ers_look_up=readfileasstring(cwd+'/sql/full_load/ers_look_up.sql')    merge_ers_look_up_names=readfileasstring(cwd+'/sql/full_load/ers_look_up_names.sql')    merge_ers_svc_trk_driver=readfileasstring(cwd+'/sql/full_load/ers_svc_trk_driver.sql')    merge_ers_predictive_truck_assignment=readfileasstring(cwd+'/sql/full_load/ers_predictive_truck_assignment.sql')    merge_ers_stage_cad_vehicle_data=readfileasstring(cwd+'/sql/full_load/merge_cad_vehicle_data.sql')    merge_ers_stage_trk_drvier_cap=readfileasstring(cwd+'/sql/full_load/merge_trk_driver_cap.sql')        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']    run()  if __name__ == '__main__':    parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)    parser.add_argument(        '--config',        required=True,        help= ('Config file name')        )    parser.add_argument(        '--productconfig',        required=True,        help= ('product Config file name')        )    parser.add_argument(        '--env',        required=True,        help= ('Enviornment to be run dev/test/prod')        )          parser.add_argument(        '--connectionprefix',        required=True,        help= ('connectionprefix either Schema')        )    args = parser.parse_args()               main(args.config,         args.productconfig,         args.env,         args.connectionprefix         )    