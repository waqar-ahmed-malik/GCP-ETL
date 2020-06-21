
import apache_beam as beam
import argparse
import logging
import os
from google.cloud import bigquery
from google.cloud import storage
from string import lower
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import os
import datetime
import fnmatch

current_date = datetime.date.today().strftime("%Y%m%d")
# current_date = "20180323"


def source_file(bucket_name,file_extension):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs()
    for blob in blobs:
#         print("*********"+blob.name)
        if blob.name.endswith(current_date+file_extension):
             print((blob.name))
             return blob.name
        
        
if __name__ == '__main__':      
        source_file("dwdev-travel",".psv")
               