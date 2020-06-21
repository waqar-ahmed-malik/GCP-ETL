 
import apache_beam as beam
import time
import jaydebeapi 
import os
import argparse
from google.cloud import bigquery
import logging
import sys
from string import lower
from google.cloud import storage as gstorage
import pandas
from oauth2client.client import GoogleCredentials





def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('dw-prod-connectsuite')
    blob = bucket.blob('IDS_CONNECTSUIT_121819.csv')

    blob.upload_from_filename('C:\\Users\ramneek.kaur\Downloads\IDS_CONNECTSUIT_121819\IDS_CONNECTSUIT_121819.csv')

    print(('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name)))