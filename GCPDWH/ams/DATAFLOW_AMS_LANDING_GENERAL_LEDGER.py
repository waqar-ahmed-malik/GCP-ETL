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

        col_list = [x.upper().replace('GENERALLEDGERREGISTER.', '') for x in list(df.columns)]

        df.columns = col_list
        df['JOBRUNID'] = job_run_id

        df = df.astype(str)

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
        '--requirements_file', '/home/airflow/gcs/data/GCPDWH/config/requirements.txt'
    ]

    try:
        pcoll = beam.Pipeline(argv=pipeline_args)
        dummy1 = pcoll | 'Initializing...' >> beam.Create(['1'])
        dummy2 = dummy1 | 'Setting up Instance...' >> beam.ParDo(SetEnv(), product_config['stagingbucket'])

        dummy2 | 'AMS_GENERAL_LEDGER to Landing' >> beam.ParDo(ReadFromSql(),
                                                               landing_ams_general_ledger,
                                                               'WORK_AMS_GENERAL_LEDGER',
                                                               incremental_date)

        p = pcoll.run()
        p.wait_until_finish()
    except Exception as e:
        logging.exception(e)
        raise

########################################################################################################################
########################################################################################################################


def main(args_config, args_productconfig, args_env, args_connectionprefix, args_incrementaldate):
    logging.getLogger().setLevel(logging.INFO)

    global py_version
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
    logging.info(job_run_id)

    job_name = "load-" + product_config['productname'] + "-" + "land" + "-" + time.strftime("%Y%m%d")

    global landing_ams_general_ledger

    landing_ams_general_ledger = readfileasstring(cwd + '/sql/LANDING_AMS_GENERAL_LEDGER.sql')

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

