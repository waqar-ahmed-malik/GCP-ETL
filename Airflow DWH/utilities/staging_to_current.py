from google.cloud import storage
from google.cloud import bigquery
import csv
import io
import logging
import re
import argparse
import json
import inflect


def change_header_names(headers: list) -> list:
    modified_headers = []
    print(headers)
    headers = [header for header in headers if len(header) != 0]
    print(headers)
    for header in headers:
        header = header.replace('(SUM)', '')
        header = re.sub(r"[^a-zA-Z0-9]+", '_', header).lower()
        if len(header) == 0:
            continue
        if header[0] == '_':
            header = header[1:]
        if header[-1] == '_':
            header = header[:-1]
        header = header.split('_')
        p = inflect.engine()
        if len(header) == 0:
            continue
        if header[0] != '0' and p.number_to_words(header[0]) in ['zero', 'zeroth']:
            pass
        else:
            header[0] = p.number_to_words(header[0])
        header = '_'.join(header)
        header = re.sub(r"[^a-zA-Z0-9]+", '_', header).lower()
        if len(header) == 0:
            continue
        if header[0] == '_':
            header = header[1:]
        if header[-1] == '_':
            header = header[:-1]
        modified_headers.append(header)
    return modified_headers


def clean_csv(buffer, clean_text_file):
    with io.TextIOWrapper(buffer) as f:
        row_skips = 0
        reader = csv.reader(f)
        for row in reader:
            if len(''.join(row)) == 0:
                row_skips += 1
            else:
                break   
        f.seek(0)
        column_count = len(next(reader))
            
        valid_column_indices = []
        for i in range(column_count):
            f.seek(0)
            if len(''.join([row[i] for row in reader])) == 0:
                continue
            else:
                valid_column_indices.append(i)
            
        f.seek(0)
            
        for row in reader:
            if (max(valid_column_indices) - min(valid_column_indices)) + 1 == len(list(set(row[min(valid_column_indices): max(valid_column_indices) + 1]))):
                headers = row[min(valid_column_indices): max(valid_column_indices) + 1]
                break
            else:
                row_skips += 1    
        f.seek(0)
        for i in range(row_skips):
            next(reader)
        
        writer = csv.writer(clean_text_file)
        for row in reader:
            writer.writerow(row[min(valid_column_indices): max(valid_column_indices) + 1])
        clean_text_file.seek(0)
        return clean_text_file


def main(args):
    logging.getLogger().setLevel(logging.INFO)
    client = storage.Client()
    bucket = client.get_bucket(args.bucket)
    for blob in bucket.list_blobs(prefix='{}/staging'.format(args.data_source)):
        if blob.name[-4:] != '.csv':
            continue
        read_file_buffer = io.BytesIO()
        blob.download_to_file(read_file_buffer)
        file_buffer.seek(0)
        logging.info("File {} read successfully from {} bucket".format(blob.name, args.bucket))
        clean_file_buffer = io.BytesIO()
        clean_text_file = io.TextIOWrapper(clean_file_buffer)
        clean_text_file = clean_csv(read_file_buffer, clean_text_file)
        write_file_buffer = io.BytesIO()
        reader = csv.reader(clean_text_file)
        headers = next(reader)
        headers = change_header_names(headers)
        logging.info('Header names have been modified.')
        with io.TextIOWrapper(write_file_buffer) as write_text_file:
            writer = csv.writer(write_text_file)
            writer.writerow(headers)
            for row in reader:
                writer.writerow(row[:len(headers)])
            write_text_file.seek(0)
            upload_blob = bucket.blob('{}'.format(blob.name.replace('staging', 'current')))
            upload_blob.upload_from_file(write_file_buffer)        
            headers = [{'name': header, 'type': 'STRING'} for header in headers]
            upload_blob = bucket.blob('{}/current/schema.json'.format(args.data_source))
            upload_blob.upload_from_string(json.dumps(headers))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--data_source',
        required=True,
        help='Name of Data Source.'
        )

    parser.add_argument(
        '--bucket',
        required=True,
        help='GCS Bucket'
        )
    args = parser.parse_args()
    main(args)