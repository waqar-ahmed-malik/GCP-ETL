import openpyxl
import pyxlsb
import csv
from google.cloud import storage
import os
import io
import logging
import re
import io
import argparse


def read_excel_from_bucket(bucket: str, blob_path: str) -> bytes:
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(blob_path)
    data = blob.download_as_string()
    return io.BytesIO(data)


def main(args):
    logging.getLogger().setLevel(logging.INFO)
    client = storage.Client()
    bucket = client.get_bucket(args.bucket)
    for blob in bucket.list_blobs(prefix='{}/staging'.format(args.data_source)):
        if blob.name == '{}/staging'.format(args.data_Source):
            continue
        read_file_buffer = io.BytesIO(blob.download_as_string())
        logging.info('{} file read successfully from GCS Bucket.'.format(blob.name))
        write_file_buffer = io.BytesIO()

        if args.source_file_extension == '.xlsx':
            excelFile = openpyxl.load_workbook(read_file_buffer)
            sheet = excelFile[args.sheet_name]
            with io.TextIOWrapper(write_file_buffer) as text_file:
                writer = csv.writer(text_file)
                for row in sheet.iter_rows():
                    row = [cell.value for cell in row]
                    writer.writerow(row)
                text_file.seek(0)
                logging.info('Data read successfully from sheet {}'.format(args.sheet_name))
                upload_blob = bucket.blob('{}/staging/{}'.format(args.data_source, blob.name.replace(args.source_file_extension, '.csv')))
                upload_blob.upload_from_file(write_file_buffer)
    elif args.source_file_extension == '.xlsb':
        with pyxlsb.open_workbook(read_file_buffer) as workbook:
            with workbook.get_sheet(args.sheet_name) as sheet:
                with io.TextIOWrapper(write_file_buffer) as text_file:
                    writer = csv.writer(text_file)
                    for row in sheet.rows():
                        row = [field[2] for field in row]
                        if len(set(row)) == 1:
                            break
                        writer.writerow(row)
                    text_file.seek(0)
                    logging.info('Data read successfully from sheet {}'.format(args.sheet_name))
                    upload_blob = bucket.blob('{}/staging/{}'.format(args.data_source, blob.name.replace(args.source_file_extension, '.csv')))
                    upload_blob.upload_from_file(write_file_buffer)
    logging.info('Data written successfully to {}'.format(args.destination_file_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--data_source',
        required=True,
        help='Full source path'
        )

    parser.add_argument(
        '--source_file_extension',
        required=True,
        help='File extension with .'
        )

    parser.add_argument(
        '--bucket',
        required=True,
        help='GCS Bucket'
        )
    
    parser.add_argument(
        '--sheet_name',
        required=True,
        help='Sheet Name'
        )
        
    args = parser.parse_args()
    main(args)