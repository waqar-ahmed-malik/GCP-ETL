from google.cloud import bigquery
from google.cloud import storage
import numpy as np
import pandas as pd
from pandas import DataFrame,Series
import logging
import datetime


def get_audit_metrics(data) -> dict:
    # Process float fields.
    d=data.dtypes[data.dtypes=='float64'].index.values
    data[d]=data[d].astype('float64')
    mean=DataFrame({'mean':data[d].mean()})
    std_dev=DataFrame({'std_dev':data[d].std()})
    missing= DataFrame({'missing':data[d].isnull().sum()})
    missing_perc=DataFrame({'missing_perc':data[d].isnull().sum()/data[d].shape[0]})
    minimum=DataFrame({'min':data[d].min()})
    maximum=DataFrame({'max':data[d].max()})
    unique=DataFrame({'unique':data[d].apply(lambda x:len(x.unique()),axis=0)})
    DQ1=pd.concat([mean,std_dev,missing,missing_perc,minimum,maximum,unique],axis=1)

    # Process integer fields.
    d=data.dtypes[data.dtypes=='int64'].index.values
    data[d]=data[d].astype('float64')
    mean=DataFrame({'mean':data[d].mean()})
    std_dev=DataFrame({'std_dev':data[d].std()})
    missing= DataFrame({'missing':data[d].isnull().sum()})
    missing_perc=DataFrame({'missing_perc':data[d].isnull().sum()/data[d].shape[0]})
    minimum=DataFrame({'min':data[d].min()})
    maximum=DataFrame({'max':data[d].max()})
    unique=DataFrame({'unique':data[d].apply(lambda x:len(x.unique()),axis=0)})
    DQ2=pd.concat([mean,std_dev,missing,missing_perc,minimum,maximum,unique],axis=1)


    # Process string fields
    c=data.dtypes[data.dtypes=='object'].index.values
    Mean=DataFrame({'mean':np.repeat('Not Applicable',len(c))},index=c)
    Std_Dev=DataFrame({'std_dev':np.repeat('Not Applicable',len(c))},index=c)
    Missing=DataFrame({'missing':data[c].isnull().sum()})
    Missing_perc=DataFrame({'missing_perc':data[c].isnull().sum()/data[c].shape[0]})
    Minimum=DataFrame({'min':np.repeat('Not Applicable',len(c))},index=c)
    Maximum=DataFrame({'max':np.repeat('Not Applicable',len(c))},index=c)
    Unique=DataFrame({'unique':data[c].apply(lambda x:len(x.unique()),axis=0)})
    DQ3=pd.concat([Mean,Std_Dev,Missing,Missing_perc,Minimum,Maximum,Unique],axis=1)

    # Process datetime fields
    c=data.dtypes[data.dtypes=='datetime64[ns, UTC]'].index.values
    Mean=DataFrame({'mean':np.repeat('Not Applicable',len(c))},index=c)
    Std_Dev=DataFrame({'std_dev':np.repeat('Not Applicable',len(c))},index=c)
    Missing=DataFrame({'missing':data[c].isnull().sum()})
    Missing_perc=DataFrame({'missing_perc':data[c].isnull().sum()/data[c].shape[0]})
    Minimum=DataFrame({'min':np.repeat('Not Applicable',len(c))},index=c)
    Maximum=DataFrame({'max':np.repeat('Not Applicable',len(c))},index=c)
    Unique=DataFrame({'unique':data[c].apply(lambda x:len(x.unique()),axis=0)})
    DQ4=pd.concat([Mean,Std_Dev,Missing,Missing_perc,Minimum,Maximum,Unique],axis=1)
    DQ=pd.concat([DQ1,DQ2, DQ3, DQ4])
    return DQ.to_dict()


def reformat_data(dataframe_dict: dict) -> dict:
    columns = []
    for metric, detail in dataframe_dict.items():
        for column, metric_value in detail.items():
            columns.append(column)
    columns = list(set(columns))

    audit_data = {}
    for column in columns:
        audit_data.__setitem__(column, {})

    for metric, detail in dataframe_dict.items():
        for column, metric_value in detail.items():
            audit_data[column].__setitem__(metric, metric_value)
    return audit_data


def insert_data_into_bq(source_audit_metrics:dict, destination_audit_metrics: dict):
    insert_query = ("""
    INSERT INTO `upwork-273613.logs.daily_job_audit` 
    ( date, integration, column_name, source, destination, load_timestamp)
    VALUES
    """
    )
    
    for column, details in source_audit_metrics.items():
        insert_query = "{}\n('{}', '{}', '{}', STRUCT('{}', '{}', '{}', '{}', '{}', '{}', '{}'), STRUCT('{}', '{}', '{}', '{}', '{}', '{}', '{}'), '{} UTC'),".format(
            insert_query, datetime.date.today(), source, column, details['mean'], details['std_dev'], details['missing'],
            details['missing_perc'], details['min'], details['max'], details['unique'],
            destination_audit_metrics[column]['mean'], destination_audit_metrics[column]['std_dev'],
            destination_audit_metrics[column]['missing'],destination_audit_metrics[column]['missing_perc'],
            destination_audit_metrics[column]['min'], destination_audit_metrics[column]['max'],
            destination_audit_metrics[column]['unique'], datetime.datetime.utcnow())
    
    client = bigquery.Client()
    query_job = client.query(insert_query[:-1])
    print(query_job.result())


def main(args):
    logging.getLogger().setLevel(logging.INFO)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(args.bucket)
    for blob in bucket.list_blobs(prefix='{}/current'.format(args.data_source)):
        try:
            df = pd.read_csv('gs://{}/current/{}'.format(bucket_name, blob.name), na_values=["","Missing"])
            source_df = pd.concat([source_df, df], ignore_index=True)
        except Exception as e:
            source_df = pd.read_csv('gs://{}/current/{}'.format(bucket_name, blob.name), na_values=["","Missing"])
    
    source_audit_metrics = get_audit_metrics(source_df)
    source_audit_metrics = reformat_data(source_audit_metrics)
    logging.info('Source Audit metrics read successfully')

    # Get BQ_Data
    
    query = "SELECT * FROM `{}.operational.{}` WHERE DATE(load_timestamp) = CURRENT_DATE".format(args.bq_project_id, args.data_source)
    data = pd.read_gbq(query, project_id=args.bq_project_id)
    del data['load_timestamp']
    destination_audit_metrics = get_audit_metrics(data)
    destination_audit_metrics = reformat_data(destination_audit_metrics)
    logging.info('Destination Audit metrics read successfully from {}'.format(args.data_source))
    
    insert_data_into_bq(source_audit_metrics, destination_audit_metrics)
    logging.info('Audit info inserted successfully in BQ.')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(
        '--data_source',
        required=True,
        help='Name of Data Source'
        )

    parser.add_argument(
        '--bucket',
        required=True,
        help='GCS Bucket'
        )
    
    parser.add_argument(
        '--bq_project_id',
        required=True,
        help='GCS Bucket'
        )
        
    args = parser.parse_args()
    main(args)

