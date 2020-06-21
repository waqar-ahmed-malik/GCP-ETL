'''
Created on March 29, 2017

@author: Rajnikant Rakesh
This module uploads file from local system/remote server to GCS Buckets using gsutil command line.
For this module to work Google Cloud SDK should be installed on the remote server.
'''

import os
import logging

def upload_gsutil(bucket_name, source_file_name, destination_file_name, keyfile=None):
    if keyfile:
        project='gcloud auth activate-service-account --key-file='+keyfile
        print(project)
        os.system(project)
    
    command="gsutil cp "+source_file_name+" gs://"+bucket_name+"/"+destination_file_name
    logging.info('Executing : {}'.format(command))
    os.system(command)
    


upload_gsutil('gcpprocess', 'C:/Users/603435/Desktop/rrakesh/divison.txt', 'divison.txt')