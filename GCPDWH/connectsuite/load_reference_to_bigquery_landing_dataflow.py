'''
Created on Sep 27, 2018
@author: Rajnikant Rakesh
This module reads data from Sqlserver and Write into Bigquery Table
It uses pandas dataframe to read and write data to BigQuery.
'''

 
import apache_beam as beam
import time
import jaydebeapi 
import os
import argparse
from google.cloud import bigquery
import logging
import sys
import base64
from google.cloud import storage as gstorage
import pandas
from oauth2client.client import GoogleCredentials

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


class setenv(beam.DoFn): 
      def process(self,context):
          os.system('gsutil cp '+product_config['stagingbucket']+'/sqljdbc41.jar /tmp/' +'&&'+ 'gsutil cp -r '+product_config['stagingbucket']+'/jdk-8u181-linux-x64.tar.gz /tmp/' )
          logging.info('Jar copied to Instance..')
          logging.info('Java Libraries copied to Instance..')
          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java')
          logging.info('Enviornment Variable set.')
          return list("1")
      
      
class readfromssql(beam.DoFn): 
      def process(self, context, inquery,targettable):
          database_user=env_config[connectionprefix+'_database_user']
#           database_password=env_config[connectionprefix+'_database_password'].decode('base64')          database_password = env_config[connectionprefix + '_database_password']          database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')
          database_host=env_config[connectionprefix+'_database_host']
          database_database=env_config[connectionprefix+'_database']
          database_port=env_config[connectionprefix+'_database_port']
          
          jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
          url = ("jdbc:sqlserver://" + database_host + ':' + database_port + ';database=' + database_database + ';user=' + database_user + ';password=' + database_password)
          jars = ["/tmp/sqljdbc41.jar"]
          libs = None
          cnx = jaydebeapi.connect(jclassname, url, jars=jars,
                            libs=libs)   
          logging.info('Connection Successful..')
          cnx.cursor()
          logging.info('Reading Sql Query from the file...')
          query = inquery.replace('v_incr_date',incrementaldate).replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
          logging.info('Query is %s',query)
          logging.info('Query submitted to SqlServer Database')

          logging.info("Started loading Table")
          for chunk in pandas.read_sql(query.replace("v_database_name", database_database), cnx, coerce_float=True, params=None, parse_dates=None, columns=None,chunksize=200000):
              chunk.apply(lambda x: x.replace('\r', ' ') if isinstance(x, str) or isinstance(x, str) else x).to_gbq(product_config['datasetname']+"."+targettable, env_config['projectid'],if_exists='replace')
             
          logging.info("Load completed...")
          return list("1")   
      
      
class runbqsql(beam.DoFn):
    def process(self, context, inputquery):
          client = bigquery.Client(project=env_config['projectid'])
          query=inputquery
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
                     '--worker_machine_type', "n1-standard-2",
                     '--extra_package', env_config['extra_package']
                    ]
   
    
    try:
        
        pcoll = beam.Pipeline(argv=pipeline_args)
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])
        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv())
        
        (dummy_env | 'M CLUB' >>  beam.ParDo(readfromssql(),landing_m_club,'WORK_M_CLUB')
        | 'CONNECTSUITE M CLUB..' >>  beam.ParDo(runbqsql(),merge_m_club))
        (dummy_env | 'M PAYMENT TYPE' >>  beam.ParDo(readfromssql(),landing_m_payment_type,'WORK_M_PAYMENT_TYPE')
        | 'CONNECTSUITE M PAYMENT TYPE..' >>  beam.ParDo(runbqsql(),merge_m_payment_type))
        (dummy_env | 'M BRANCH' >>  beam.ParDo(readfromssql(),landing_m_branch,'WORK_M_BRANCH')
        | 'CONNECTSUITE M BRANCH..' >>  beam.ParDo(runbqsql(),merge_m_branch))
        (dummy_env | 'M BRANCH CLUB' >>  beam.ParDo(readfromssql(),landing_m_branch_club,'WORK_M_BRANCH_CLUB')
        | 'CONNECTSUITE M BRANCH CLUB..' >>  beam.ParDo(runbqsql(),merge_m_branch_club))
        (dummy_env | 'M BRANCH TERRITORY' >>  beam.ParDo(readfromssql(),landing_m_branch_territory,'WORK_M_BRANCH_TERRITORY')
        | 'CONNECTSUITE M BRANCH TERRITORY..' >>  beam.ParDo(runbqsql(),merge_m_branch_territory))
        (dummy_env | 'M SALES AGENT' >>  beam.ParDo(readfromssql(),landing_m_sales_agent,'WORK_M_SALES_AGENT')
        | 'CONNECTSUITE M SALES AGENT..' >>  beam.ParDo(runbqsql(),merge_m_sales_agent))
        
        (dummy_env | 'SAM WORKGROUPS' >>  beam.ParDo(readfromssql(),landing_sam_workgroups,'WORK_SAM_WORKGROUPS')
        | 'CONNECTSUITE SAM WORKGROUP..' >>  beam.ParDo(runbqsql(),merge_sam_workgroups))
        (dummy_env | 'SAM WORKGROUP BRANCHES' >>  beam.ParDo(readfromssql(),landing_sam_workgroup_branches_map,'WORK_SAM_WORKGROUP_BRANCHES_MAP')
        | 'CONNECTSUITE SAM WORKGROUP BRANCHES..' >>  beam.ParDo(runbqsql(),merge_sam_workgroup_branches_map))
        (dummy_env | 'SAM WORKGROUP SUPERVISORS' >>  beam.ParDo(readfromssql(),landing_sam_workgroup_supervisors_map,'WORK_SAM_WORKGROUP_SUPERVISORS_MAP')
        | 'CONNECTSUITE SAM WORKGROUP SUPERVISOR..' >>  beam.ParDo(runbqsql(),merge_sam_workgroup_supervisors_map))
        (dummy_env | 'POS INV SKU' >>  beam.ParDo(readfromssql(),landing_pos_inv_sku,'WORK_POS_INV_SKU')
        | 'CONNECTSUITE POS INV SKU..' >>  beam.ParDo(runbqsql(),merge_pos_inv_sku))
        (dummy_env | 'POS CLB OFC SLS TAX' >>  beam.ParDo(readfromssql(),landing_pos_clb_ofc_sls_tax,'WORK_POS_CLB_OFC_SLS_TAX')
        | 'CONNECTSUITE POS CLB OFC SLS TAX..' >>  beam.ParDo(runbqsql(),merge_pos_clb_ofc_sls_tax))
        (dummy_env | 'CS BRANCH' >>  beam.ParDo(readfromssql(),landing_cs_branch,'WORK_CS_BRANCH')
        | 'CONNECTSUITE CS BRANCH..' >>  beam.ParDo(runbqsql(),merge_cs_branch))
        (dummy_env | 'CS WORKGROUP BRANCHES' >>  beam.ParDo(readfromssql(),landing_cs_branchzips,'WORK_CS_BRANCHZIPS')
        | 'CONNECTSUITE CS BRANCHZIPS..' >>  beam.ParDo(runbqsql(),merge_cs_branchzips))
        (dummy_env | 'CS EMPLOYEE' >>  beam.ParDo(readfromssql(),landing_cs_employees,'WORK_CS_EMPLOYEES')
        | 'CONNECTSUITE CS EMPLOYEE..' >>  beam.ParDo(runbqsql(),merge_cs_employees))
        (dummy_env | 'CS EMPLOYEE BRANCHES' >>  beam.ParDo(readfromssql(),landing_cs_employeebranch,'WORK_CS_EMPLOYEEBRANCH')
        | 'CONNECTSUITE CS EMPLOYEEBRANCH..' >>  beam.ParDo(runbqsql(),merge_cs_employeebranch))
        (dummy_env | 'CS ROLES' >>  beam.ParDo(readfromssql(),landing_cs_roles,'WORK_CS_ROLES')
        | 'CONNECTSUITE CS ROLES..' >>  beam.ParDo(runbqsql(),merge_cs_roles))
        (dummy_env | 'CS BRANCH ZIP MAP' >>  beam.ParDo(readfromssql(),landing_cs_branch_zip_map,'WORK_CS_BRANCH_ZIP_MAP')
        | 'CONNECTSUITE CS BRANCH ZIP MAP..' >>  beam.ParDo(runbqsql(),merge_cs_branch_zip_map))

        p = pcoll.run()
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
    global sqlstring
    global jobrunid
    jobrunid=os.getpid()
    logging.info('Job Run Id is %d',jobrunid)
    global jobname
    jobname="load-"+product_config['productname']+"-"+"landingreference"+"-"+time.strftime("%Y%m%d")
    logging.info('Job Name is %s',jobname)
    
    global landing_cs_branch
    global landing_cs_branchzips
    global landing_cs_branch_zip_map
    global landing_cs_employees
    global landing_cs_employeebranch
    global landing_cs_roles
    global landing_m_branch
    global landing_m_branch_club
    global landing_m_branch_territory
    global landing_m_club
    global landing_m_payment_type
    global landing_m_sales_agent
    global landing_sam_workgroups
    global landing_sam_workgroup_branches_map
    global landing_sam_workgroup_supervisors_map
    global landing_pos_inv_sku
    global landing_pos_clb_ofc_sls_tax
     
    landing_cs_branch= readfileasstring(cwd + '/sql/reference/work_cs_branch.sql')
    landing_cs_branchzips= readfileasstring(cwd + '/sql/reference/work_cs_branchzips.sql')
    landing_cs_branch_zip_map= readfileasstring(cwd + '/sql/reference/work_cs_branch_zip_map.sql')
    landing_cs_employees= readfileasstring(cwd + '/sql/reference/work_cs_employees.sql')
    landing_cs_employeebranch= readfileasstring(cwd + '/sql/reference/work_cs_employeebranch.sql')
    landing_cs_roles= readfileasstring(cwd + '/sql/reference/work_cs_roles.sql')
    landing_m_branch= readfileasstring(cwd + '/sql/reference/work_m_branch.sql')
    landing_m_branch_club= readfileasstring(cwd + '/sql/reference/work_m_branch_club.sql')
    landing_m_branch_territory= readfileasstring(cwd + '/sql/reference/work_m_branch_territory.sql')
    landing_m_club= readfileasstring(cwd + '/sql/reference/work_m_club.sql')
    landing_m_payment_type= readfileasstring(cwd + '/sql/reference/work_m_payment_type.sql')
    landing_m_sales_agent= readfileasstring(cwd + '/sql/reference/work_m_sales_agent.sql')
    landing_sam_workgroups= readfileasstring(cwd + '/sql/reference/work_sam_workgroups.sql')
    landing_sam_workgroup_branches_map= readfileasstring(cwd + '/sql/reference/work_sam_workgroup_branches_map.sql')
    landing_sam_workgroup_supervisors_map= readfileasstring(cwd + '/sql/reference/work_sam_workgroup_supervisors_map.sql')
    landing_pos_inv_sku= readfileasstring(cwd + '/sql/reference/work_pos_inv_sku.sql')
    landing_pos_clb_ofc_sls_tax= readfileasstring(cwd + '/sql/reference/work_pos_clb_ofc_sls_tax.sql')    
    
    
    
    global merge_cs_branch
    global merge_cs_branchzips
    global merge_cs_branch_zip_map
    global merge_cs_employees
    global merge_cs_employeebranch
    global merge_cs_roles
    global merge_m_branch
    global merge_m_branch_club
    global merge_m_branch_territory
    global merge_m_club
    global merge_m_payment_type
    global merge_m_sales_agent
    global merge_sam_workgroups
    global merge_sam_workgroup_branches_map
    global merge_sam_workgroup_supervisors_map
    global merge_pos_inv_sku
    global merge_pos_clb_ofc_sls_tax
    
    merge_cs_branch= readfileasstring(cwd + '/sql/reference/connectsuite_cs_branch.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_cs_branchzips= readfileasstring(cwd + '/sql/reference/connectsuite_cs_branchzips.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_cs_branch_zip_map= readfileasstring(cwd + '/sql/reference/connectsuite_cs_branch_zip_map.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_cs_employees= readfileasstring(cwd + '/sql/reference/connectsuite_cs_employees.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_cs_employeebranch= readfileasstring(cwd + '/sql/reference/connectsuite_cs_employeebranch.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_cs_roles= readfileasstring(cwd + '/sql/reference/connectsuite_cs_roles.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_m_branch= readfileasstring(cwd + '/sql/reference/connectsuite_m_branch.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_m_branch_club= readfileasstring(cwd + '/sql/reference/connectsuite_m_branch_club.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_m_branch_territory= readfileasstring(cwd + '/sql/reference/connectsuite_m_branch_territory.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_m_club= readfileasstring(cwd + '/sql/reference/connectsuite_m_club.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_m_payment_type= readfileasstring(cwd + '/sql/reference/connectsuite_m_payment_type.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_m_sales_agent= readfileasstring(cwd + '/sql/reference/connectsuite_m_sales_agent.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_sam_workgroups= readfileasstring(cwd + '/sql/reference/connectsuite_sam_workgroups.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_sam_workgroup_branches_map= readfileasstring(cwd + '/sql/reference/connectsuite_sam_workgroup_branches_map.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_sam_workgroup_supervisors_map= readfileasstring(cwd + '/sql/reference/connectsuite_sam_workgroup_supervisors_map.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_pos_inv_sku= readfileasstring(cwd + '/sql/reference/connectsuite_pos_inv_sku.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)
    merge_pos_clb_ofc_sls_tax= readfileasstring(cwd + '/sql/reference/connectsuite_pos_clb_ofc_sls_tax.sql').replace('jobrunid',str(jobrunid)).replace('jobname', jobname)

    
    
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=env_config['service_account_key_file']
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
         args.incrementaldate)    