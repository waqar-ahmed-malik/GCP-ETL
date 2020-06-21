import pysftp
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from io import BytesIO
import logging
import argparse


def get_max_load_datetime(data_source: str) -> str:
    client = bigquery.Client()
    query = "SELECT SUBSTR(CAST(DATETIME(MAX(load_timestamp)) AS STRING), 0,19) AS max_load_datetime FROM `operational.{}`".format(data_source)
    query_job = client.query(query)
    for row in query_job.result():
        return str(row['max_load_datetime'])
    return '2020-01-01 00:00:00'


def main(args):
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Cleaned the Staging folder Successfully')
    max_load_datetime = datetime.strptime(get_max_load_datetime(args.data_source), '%Y-%m-%d %H:%M:%S')
    logging.info('{} table contains data until {}'.format(args.data_sorce, max_load_datetime))
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as conn:
        for attr in conn.listdir_attr(args.source_directory):
            file_modified_datetime = datetime.utcfromtimestamp(attr.st_mtime)
            if file_modified_datetime > max_load_datetime:
                source_file_path = '{}/{}'.format(args.source_directory, attr.filename)
                file_buffer = BytesIO()
                conn.getfo(source_file_path, file_buffer)
                file_buffer.seek(0)
                logging.info('Successfully Read File {} from SFTP.'.format(source_file_path))
                destination_blob_path = '{}/staging/{}'.format(args.data_Source, attr.filename)
                client = storage.Client()
                bucket = client.get_bucket(args.bucket)
                blob = bucket.blob(destination_blob_path)
                blob.upload_from_file(file_buffer)
                logging.info('Successfully written file {} to GCS Bucket'.format(destination_blob_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--source_directory',
        required=True,
        help='SFTP Source Folder'
        )

    parser.add_argument(
        '--data_source',
        required=True,
        help='Name of Data Source'
        )

    parser.add_argument(
        '--sftp_host',
        required=True,
        help='SFTP Host'
        )

    parser.add_argument(
        '--sftp_username',
        required=True,
        help='SFTP Username'
        )

    parser.add_argument(
        '--sftp_password',
        required=True,
        help='SFTP Password'
        )

    parser.add_argument(
        '--bucket',
        required=True,
        help='GCS Bucket'
        )
        
    args = parser.parse_args()
    main(args)