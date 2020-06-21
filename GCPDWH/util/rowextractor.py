
import apache_beam as beam
import argparse
import logging
import os
from google.cloud import bigquery
from string import lower
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

global separatorchar
global tablefields

separatorchar='|'
tablefields = []

exec(compile(open('C://GCP//GCPDWH//util//readtableschema.py').read(), 'C://GCP//GCPDWH//util//readtableschema.py', 'exec'), globals())
tablefields = readtableschema('aaadata-181822', 'LANDING', 'TRAVELWARE_TST')

def rowextractor(inputdata):
    """reads input and splits to relevant row format"""
    logging.info("Starting row processing in row extractor")
    try:
        rec = {}
        i=0
        row = inputdata.split(separatorchar)
        for field in tablefields:
            if row[i]:
                rec[field]= row[i].strip('"').strip('\n').strip('\r')
            else:
                rec[field]= None
            i=i+1
            #logging.info(field.name+":"+rec[field.name])
        #logging.info(rec)
    except:
        logging.error("Error in processing row")
        raise
    return rec



print((rowextractor("C://gcp_key//AAANCNU_Data_20180314.psv")))
