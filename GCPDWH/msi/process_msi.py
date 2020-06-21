'''

Created on JuAugly 14, 2018

@author: Atul Guleria

'''

from datetime import datetime, timedelta

from datetime import datetime

import os

import argparse

from google.cloud import bigquery

from google.cloud import storage

import logging



now = datetime.now()



cwd = os.path.dirname(os.path.abspath(__file__))

if cwd.find("\\") > 0:

    folderup = cwd.rfind("\\")

    utilpath = cwd[:folderup] + "\\util\\"

    msipath = cwd[:folderup] + "\\msi\\"

    # Else go with linux path

else:

    folderup = cwd.rfind("/")

    utilpath = cwd[:folderup] + "/util/"

    msipath = cwd[:folderup] + "/msi/"





def main(args_config, args_productconfig, args_env, args_inputdate):

    logging.getLogger().setLevel(logging.INFO)

    global env

    env = args_env

    global config

    config = args_config

    global productconfig

    productconfig = args_productconfig

    global env_config

    exec(compile(open(utilpath + "readconfig.py").read(), utilpath + "readconfig.py", 'exec'), globals())

    env_config = readconfig(config, env)

    global product_config

    product_config = readconfig(productconfig, env)

    global fileid

    global jobrunid

    jobrunid = os.getpid()

    logging.info('Job Run Id is %d', jobrunid)

    global jobname

    global inputdate

    inputdate = args_inputdate

    datetime.today() - timedelta(days=1)

    jobname = "load_" + product_config['productname']

    logging.info('Job Name is %s', jobname)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = env_config["service_account_key_file"]

    client = storage.Client(env_config['projectid'])

    # bucket = client.bucket(product_config['bucket'])

    logging.info('Started Loading Files  ....')

    bucket_list = ["-msi-results-msi", "-napatracs--msi", "-mac-transactions-msi", "-msi-skip-msi",

                   "-dsra-transactions-msi"]

    for loc in bucket_list:

        bucket_name = "dw-" + env + loc

        print(bucket_name)

        bucket = client.bucket(bucket_name)

        for blob in bucket.list_blobs(prefix='current/'):

            print(blob)

            filename = blob.name.split('/')[0] + '/' + blob.name.split('/')[1]  # +'/'+blob.name.split('/')[2]

            # print filename

            file = 'gs://' + bucket_name + '/' + filename

            # print file

            flag = 1

            if 'Export.' + inputdate in filename:

                stg_table_name = 'WORK_ERS_SURVEY'

                skip_lead_rows = '0'

                delimiter = '^|'

                print(" WORK_ERS_SURVEY")

            elif 'NapaTracs.' + inputdate + '.csv' in filename:

                stg_table_name = 'WORK_NAPA'

                skip_lead_rows = '1'

                delimiter = '^|'

                print(" WORK_NAPA")

            elif 'Report04.' + inputdate in filename:

                stg_table_name = 'WORK_MAC_TRIPTIK'

                skip_lead_rows = '0'

                delimiter = '^|'

                print("WORK_MAC_TRIPTIK")

            elif 'ers_export_' + inputdate in filename:

                stg_table_name = 'WORK_MSI_ERS'

                skip_lead_rows = '1'

                delimiter = '^|'

                print("WORK_MSI_ERS")

            elif 'Skip_File.' + inputdate in filename:

                stg_table_name = 'WORK_SKIP_FILE_MEM_NUM'

                skip_lead_rows = '1'

                print("WORK_SKIP_FILE_MEM_NUM")

            else:

                flag = 0



            if flag == 1:

                logging.info("Stage table name is " + stg_table_name)

                if stg_table_name == 'WORK_SKIP_FILE_MEM_NUM':

                    loadwrktables = "python " + utilpath + "load_csv_to_bigquery_bqsdk.py " + "--config config.properties --productconfig msi.properties --env " + env + " --targettable " + stg_table_name + " --filename " + filename + " --delimiter \\t --deposition WRITE_APPEND --skiprows " + skip_lead_rows

                    logging.info('load Skip file' + loadwrktables)

                    os.system(loadwrktables)

                else:

                    loadwrktables = "python " + msipath + "load_msi_to_bigquery_landing.py " + "--config config.properties --productconfig msi.properties --env " + env + " --input " + '"' + file + '"' + " --separator " + delimiter + " --stripheader " + skip_lead_rows + " --stripdelim 0  --addaudit 1   --output " + stg_table_name + "  --writeDeposition WRITE_APPEND"

                    logging.info('MSI work table loading' + loadwrktables)

                    os.system(loadwrktables)

    logging.info('All files Loaded  ....')





if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(

        '--config',

        required=True,

        help=('Config file name')

    )

    parser.add_argument(

        '--productconfig',

        required=True,

        help=('product Config file name')

    )

    parser.add_argument(

        '--env',

        required=True,

        help=('Enviornment to be run dev/test/prod')

    )

    parser.add_argument(

        '--inputdate',

        required=True,

        help=('Enviornment to be run dev/test/prod')

    )



    args = parser.parse_args()



main(args.config,

     args.productconfig,

     args.env,

     args.inputdate)

