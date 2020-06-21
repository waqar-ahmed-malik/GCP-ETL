from google.cloud import storage
import logging
import argparse


def clean_folder(folder_path: str, bucket: str):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    for blob in bucket.list_blobs(prefix=folder_path):
        blob.delete()


def main(args):
    logging.getLogger().setLevel(logging.INFO)
    clean_folder('{}/staging'.format(args.data_source), args.bucket)
    logging.info('{}/staging folder cleaned successfully.')
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(
        '--data_source',
        required=True,
        help='Full destination path'
        )

    parser.add_argument(
        '--bucket',
        required=True,
        help='GCS Bucket'
        )
        
    args = parser.parse_args()
    main(args)