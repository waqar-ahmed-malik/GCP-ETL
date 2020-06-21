from __future__ import generators

import os
import sys
import time
import logging
import argparse
import jaydebeapi
import pandas as pd
import apache_beam as beam

from google.cloud import storage
from google.cloud import bigquery
from oauth2client.client import GoogleCredentials

########################################################################################################################
########################################################################################################################

cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:
    folderup = cwd.rfind("\\")
    utilpath = cwd[:folderup]+"\\util\\"
    #Else go with linux path
else:
    folderup = cwd.rfind("/")
    utilpath = cwd[:folderup]+"/util/"

print(cwd)
print(utilpath)

########################################################################################################################
########################################################################################################################


def readfileasstring(sqlfile):
    """Read any text file and return text as a string. This function is used to read .sql and .schema files"""
    with open(sqlfile, "r") as file:
        sqltext = file.read().strip("\n").strip("\r")
    return sqltext

########################################################################################################################
########################################################################################################################


class SetEnv(beam.DoFn):
    def process(self, context, staging_bucket_location):
        import os
        import logging

        os.system('gsutil cp ' + staging_bucket_location + '/sqljdbc41.jar /tmp/' + '&&' +
                  'gsutil cp -r ' + staging_bucket_location + '/jdk-8u181-linux-x64.tar.gz /tmp/')
        logging.info('Jar copied to Instance...')
        logging.info('Java Libraries copied to Instance...')

        os.system('mkdir -p /usr/lib/jvm && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm && '
                  'update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && '
                  'update-alternatives --config java')
        logging.info('Environment Variable set.')

        return list("1")

########################################################################################################################
########################################################################################################################


class ReadFromSql(beam.DoFn):
    def process(self, context, inquery, targettable, inc_date):
        import base64
        import logging
        import jaydebeapi
        import pandas as pd

        database_host = env_config[connection_prefix + '_database_host']
        database_port = env_config[connection_prefix + '_database_port']
        database_database = env_config[connection_prefix + '_database']
        database_user = env_config[connection_prefix + '_database_user']

        ## py_version == 3
        database_password = env_config[connection_prefix + '_database_password']
        database_password = str(base64.b64decode(database_password.encode('utf-8')), 'utf-8')

        ## py_version == 2
        # database_password = env_config[connection_prefix + '_database_password']
        # database_password = database_password.decode('base64')

        jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        url = ("jdbc:sqlserver://" + database_host + ":" + database_port +
               ";database=" + database_database +
               ";user=" + database_user +
               ";password=" + database_password)
        jars = ["/tmp/sqljdbc41.jar"]
        libs = None

        cnx = jaydebeapi.connect(jclassname, url, jars=jars, libs=libs)
        logging.info("Connection Successful...")

        cnx.cursor()
        logging.info("Reading Sql Query from the file...")

        inquery = inquery.replace("xxx_replace_this_xxx", inc_date)
        logging.info('Query is %s', inquery)
        logging.info('Query submitted to SqlServer Database')

        logging.info("Started loading table")

        df = pd.read_sql(sql=inquery, con=cnx)

        col_list = [x.upper() for x in list(df.columns)]

        df.columns = col_list
        df['JOBRUNID'] = job_run_id

        df = df.astype(str)

        df = df.replace(to_replace='None', value='') \
            .replace(to_replace='NaT', value='') \
            .replace(to_replace='nan', value='') \
            .replace(to_replace='\s\s+$', value='', regex=True)

        df.to_gbq('LANDING.' + targettable,
                 project_id=str(env_config['projectid']),
                  if_exists='replace')

        logging.info("Load completed...")
        return list("1")

########################################################################################################################
########################################################################################################################


def run():
    pipeline_args = [
        '--save_main_session', 'True',
        '--runner', str(env_config['runner']),
        '--job_name', job_name,
        '--project', str(env_config['projectid']),
        '--temp_location', str(product_config['tempbucket']),
        '--staging_location', str(product_config['stagingbucket']),
        '--region', str(env_config['region']),
        '--zone', str(env_config['zone']),
        '--autoscaling_algorithm', str(env_config['autoscaling_algorithm']),
        '--num_workers', str(env_config['num_workers']),
        '--max_num_workers', str(env_config['max_num_workers']),
        '--network', str(env_config['network']),
        '--subnetwork', str(env_config['subnetwork']),
        '--machine_type', 'n1-standard-2',
        '--requirements_file', '/home/airflow/gcs/data/GCPDWH/config/requirements.txt',
    ]

    try:
        pcoll = beam.Pipeline(argv=pipeline_args)
        dummy1 = pcoll | 'Initializing...' >> beam.Create(['1'])
        dummy2 = dummy1 | 'Setting up Instance...' >> beam.ParDo(SetEnv(), product_config['stagingbucket'])

        dummy2 | 'AMS Branch to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_Branch, 'WORK_AMS_BRANCH', incremental_date)
        dummy2 | 'AMS CdPolicyLineType to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_CdPolicyLineType, 'WORK_AMS_CD_POLICY_LINE_TYPE', incremental_date)
        dummy2 | 'AMS Client to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_Client, 'WORK_AMS_CLIENT', incremental_date)
        dummy2 | 'AMS ClientAgencyBranchJT to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_ClientAgencyBranchJT, 'WORK_AMS_CLIENT_AGENCY_BRANCH_JT', incremental_date)
        dummy2 | 'AMS Company to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_Company, 'WORK_AMS_COMPANY', incremental_date)
        dummy2 | 'AMS ConfigureLKLanguageResource to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_ConfigureLKLanguageResource, 'WORK_AMS_CONFIGURE_LK_LANGUAGE_RESOURCE', incremental_date)
        dummy2 | 'AMS ContactAddress to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_ContactAddress, 'WORK_AMS_CONTACT_ADDRESS', incremental_date)
        dummy2 | 'AMS ContactClass to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_ContactClass, 'WORK_AMS_CONTACT_CLASS', incremental_date)
        dummy2 | 'AMS ContactName to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_ContactName, 'WORK_AMS_CONTACT_NAME', incremental_date)
        dummy2 | 'AMS ContactNumber to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_ContactNumber, 'WORK_AMS_CONTACT_NUMBER', incremental_date)
        dummy2 | 'AMS Coverage to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_Coverage, 'WORK_AMS_COVERAGE', incremental_date)
        dummy2 | 'AMS Employee to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_Employee, 'WORK_AMS_EMPLOYEE', incremental_date)
        dummy2 | 'AMS EntityEmployeeJT to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_EntityEmployeeJT, 'WORK_AMS_ENTITY_EMPLOYEE_JT', incremental_date)
        dummy2 | 'AMS Line to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_Line, 'WORK_AMS_LINE', incremental_date)
        dummy2 | 'AMS LineBrokerProducer to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_LineBrokerProducer, 'WORK_AMS_LINE_BROKER_PRODUCER', incremental_date)
        dummy2 | 'AMS LineImage to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_LineImage, 'WORK_AMS_LINE_IMAGE', incremental_date)
        dummy2 | 'AMS LocationCoverage to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_LocationCoverage, 'WORK_AMS_LOCATION_COVERAGE', incremental_date)
        dummy2 | 'AMS PAAdditionalCoverage to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_PAAdditionalCoverage, 'WORK_AMS_PA_ADDITIONAL_COVERAGE', incremental_date)
        dummy2 | 'AMS PADriver to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_PADriver, 'WORK_AMS_PA_DRIVER', incremental_date)
        dummy2 | 'AMS PAImage to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_PAImage, 'WORK_AMS_PA_IMAGE', incremental_date)
        dummy2 | 'AMS Policy to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_Policy, 'WORK_AMS_POLICY', incremental_date)
        dummy2 | 'AMS RSAdditionalInterest to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_RSAdditionalInterest, 'WORK_AMS_RS_ADDITIONAL_INTEREST', incremental_date)
        dummy2 | 'AMS RSImage to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_RSImage, 'WORK_AMS_RS_IMAGE', incremental_date)
        dummy2 | 'AMS SecurityUser to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_SecurityUser, 'WORK_AMS_SECURITY_USER', incremental_date)
        dummy2 | 'AMS SecurityUserStructureCombinationJT to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_SecurityUserStructureCombinationJT, 'WORK_AMS_SECURITY_USER_STRUCTURE_COMBINATION_JT', incremental_date)
        dummy2 | 'AMS StructureCombination to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_StructureCombination, 'WORK_AMS_STRUCTURE_COMBINATION', incremental_date)
        dummy2 | 'AMS TransCode to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_TransCode, 'WORK_AMS_TRANS_CODE', incremental_date)
        dummy2 | 'AMS TransDetail to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_TransDetail, 'WORK_AMS_TRANS_DETAIL', incremental_date)
        dummy2 | 'AMS TransHead to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_TransHead, 'WORK_AMS_TRANS_HEAD', incremental_date)
        dummy2 | 'AMS VehicleCoverage to Landing' >> beam.ParDo(ReadFromSql(), landing_ams_VehicleCoverage, 'WORK_AMS_VEHICLE_COVERAGE', incremental_date)

        p = pcoll.run()
        p.wait_until_finish()
    except Exception as e:
        logging.exception(e)
        raise

########################################################################################################################
########################################################################################################################


def main(args_config, args_productconfig, args_env, args_connectionprefix, args_incrementaldate):
    logging.getLogger().setLevel(logging.INFO)

    global env_config
    global product_config
    global connection_prefix
    global incremental_date
    global job_run_id
    global job_name

    ## py_version == 3
    exec(compile(open(utilpath + 'readconfig.py', "rb").read(), 'readconfig.py', 'exec'), globals())

    ## py_version == 2
    # execfile(utilpath + 'readconfig.py', globals())

    env_config = readconfig(args_config, args_env)
    product_config = readconfig(args_productconfig, args_env)
    connection_prefix = args_connectionprefix
    incremental_date = args_incrementaldate
    job_run_id = os.getpid()
    job_name = "load-" + product_config['productname'] + "-1-30-tables-" + "land" + "-" + time.strftime("%Y%m%d")

    global landing_ams_Branch
    global landing_ams_CdPolicyLineType
    global landing_ams_Client
    global landing_ams_ClientAgencyBranchJT
    global landing_ams_Company
    global landing_ams_ConfigureLKLanguageResource
    global landing_ams_ContactAddress
    global landing_ams_ContactClass
    global landing_ams_ContactName
    global landing_ams_ContactNumber
    global landing_ams_Coverage
    global landing_ams_Employee
    global landing_ams_EntityEmployeeJT
    global landing_ams_Line
    global landing_ams_LineBrokerProducer
    global landing_ams_LineImage
    global landing_ams_LocationCoverage
    global landing_ams_PAAdditionalCoverage
    global landing_ams_PADriver
    global landing_ams_PAImage
    global landing_ams_Policy
    global landing_ams_RSAdditionalInterest
    global landing_ams_RSImage
    global landing_ams_SecurityUser
    global landing_ams_SecurityUserStructureCombinationJT
    global landing_ams_StructureCombination
    global landing_ams_TransCode
    global landing_ams_TransDetail
    global landing_ams_TransHead
    global landing_ams_VehicleCoverage

    landing_ams_Branch = readfileasstring(cwd + '/sql/LANDING_AMS_BRANCH.sql')
    landing_ams_CdPolicyLineType = readfileasstring(cwd + '/sql/LANDING_AMS_CD_POLICY_LINE_TYPE.sql')
    landing_ams_Client = readfileasstring(cwd + '/sql/LANDING_AMS_CLIENT.sql')
    landing_ams_ClientAgencyBranchJT = readfileasstring(cwd + '/sql/LANDING_AMS_CLIENT_AGENCY_BRANCH_JT.sql')
    landing_ams_Company = readfileasstring(cwd + '/sql/LANDING_AMS_COMPANY.sql')
    landing_ams_ConfigureLKLanguageResource = readfileasstring(cwd + '/sql/LANDING_AMS_CONFIGURE_LK_LANGUAGE_RESOURCE.sql')
    landing_ams_ContactAddress = readfileasstring(cwd + '/sql/LANDING_AMS_CONTACT_ADDRESS.sql')
    landing_ams_ContactClass = readfileasstring(cwd + '/sql/LANDING_AMS_CONTACT_CLASS.sql')
    landing_ams_ContactName = readfileasstring(cwd + '/sql/LANDING_AMS_CONTACT_NAME.sql')
    landing_ams_ContactNumber = readfileasstring(cwd + '/sql/LANDING_AMS_CONTACT_NUMBER.sql')
    landing_ams_Coverage = readfileasstring(cwd + '/sql/LANDING_AMS_COVERAGE.sql')
    landing_ams_Employee = readfileasstring(cwd + '/sql/LANDING_AMS_EMPLOYEE.sql')
    landing_ams_EntityEmployeeJT = readfileasstring(cwd + '/sql/LANDING_AMS_ENTITY_EMPLOYEE_JT.sql')
    landing_ams_Line = readfileasstring(cwd + '/sql/LANDING_AMS_LINE.sql')
    landing_ams_LineBrokerProducer = readfileasstring(cwd + '/sql/LANDING_AMS_LINE_BROKER_PRODUCER.sql')
    landing_ams_LineImage = readfileasstring(cwd + '/sql/LANDING_AMS_LINE_IMAGE.sql')
    landing_ams_LocationCoverage = readfileasstring(cwd + '/sql/LANDING_AMS_LOCATION_COVERAGE.sql')
    landing_ams_PAAdditionalCoverage = readfileasstring(cwd + '/sql/LANDING_AMS_PA_ADDITIONAL_COVERAGE.sql')
    landing_ams_PADriver = readfileasstring(cwd + '/sql/LANDING_AMS_PA_DRIVER.sql')
    landing_ams_PAImage = readfileasstring(cwd + '/sql/LANDING_AMS_PA_IMAGE.sql')
    landing_ams_Policy = readfileasstring(cwd + '/sql/LANDING_AMS_POLICY.sql')
    landing_ams_RSAdditionalInterest = readfileasstring(cwd + '/sql/LANDING_AMS_RS_ADDITIONAL_INTEREST.sql')
    landing_ams_RSImage = readfileasstring(cwd + '/sql/LANDING_AMS_RS_IMAGE.sql')
    landing_ams_SecurityUser = readfileasstring(cwd + '/sql/LANDING_AMS_SECURITY_USER.sql')
    landing_ams_SecurityUserStructureCombinationJT = readfileasstring(cwd + '/sql/LANDING_AMS_SECURITY_USER_STRUCTURE_COMBINATION_JT.sql')
    landing_ams_StructureCombination = readfileasstring(cwd + '/sql/LANDING_AMS_STRUCTURE_COMBINATION.sql')
    landing_ams_TransCode = readfileasstring(cwd + '/sql/LANDING_AMS_TRANS_CODE.sql')
    landing_ams_TransDetail = readfileasstring(cwd + '/sql/LANDING_AMS_TRANS_DETAIL.sql')
    landing_ams_TransHead = readfileasstring(cwd + '/sql/LANDING_AMS_TRANS_HEAD.sql')
    landing_ams_VehicleCoverage = readfileasstring(cwd + '/sql/LANDING_AMS_VEHICLE_COVERAGE.sql')

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = env_config['service_account_key_file']
    run()


########################################################################################################################
########################################################################################################################


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--config',
                        default='config.properties',
                        required=True,
                        help='Config file name'
                        )
    parser.add_argument('--productconfig',
                        default='ams.properties',
                        required=True,
                        help='product Config file name'
                        )
    parser.add_argument('--env',
                        default='dev',
                        required=True,
                        help='Enviornment to be run dev/test/prod'
                        )
    parser.add_argument('--connectionprefix',
                        default='ams',
                        required=True,
                        help='connectionprefix either Schema'
                        )
    parser.add_argument('--incrementaldate',
                        default='1',
                        required=True,
                        help='Incremental Data Pull Filter date'
                        )

    args = parser.parse_args()

    main(args.config,
         args.productconfig,
         args.env,
         args.connectionprefix,
         args.incrementaldate)

