'''
Created on Apr 19, 2017
@author: Rajnikant Rakesh
This module tests getbigqueryresult module.
It reads data from a bigquery table via sqlquery parameter and
read data from bigquery tables 
It depends on config.properties and product config properties for details of load
Please make sure while passing projectid to module you pass dev env projectid
'''
import argparse
import time
import uuid
from google.cloud import bigquery
from google.cloud.bigquery import Table
from google.cloud.bigquery.schema import SchemaField

import logging
def getbigqueryresult(query,projectid):
    #Create a table to load
    logging.info(query)
    client = bigquery.Client(project=projectid)
    query_job = client.run_async_query(str(uuid.uuid4()), query)
    query_job.use_legacy_sql = False
    query_job.begin()
    logging.info(query_job.state)
    #print 'Query Status is {}'.format(query_job.state)
    #wait_for_job(query_job)
    while query_job.state != 'DONE':
        query_job.reload()  # Refreshes the state via a GET request.
        time.sleep(1)
    logging.info(query_job.state)
    #print 'Query Status is {}'.format(query_job.state)
    query_results = bigquery.query.QueryResults('', client)
    query_results._properties['jobReference'] = {
                                                 'jobId': query_job.name,
                                                 'projectId': query_job.project
                                                 }

    rows, total_rows, page_token = query_results.fetch_data() #(max_results=10,page_token=page_token)
    schema= query_results.schema
    resultset=[]
    for row in rows:
        resultrow={}
        i=0
        for field in schema:
            resultrow[field.name]=str(row[i])
            i=i+1
        resultset.append(resultrow)
            

    return resultset
 
    
                
