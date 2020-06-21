'''
Created on April 04, 2018

@author: Rajnikant Rakesh
This module uploads file from local system/remote server to GCS Buckets using gsutil command line.
For this module to work Google Cloud SDK should be installed on the remote server.
'''

import paramiko
import datetime
import os
import logging

# host = 
port = 22
transport = paramiko.Transport((host, port))
password = "603435"
username = "Hadoop@2690"
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)


#ssh_client=paramiko.SSHClient()
#ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#ssh_client.connect(hostname='10.140.231.66',username='603435',password='Hadoop@2690')
