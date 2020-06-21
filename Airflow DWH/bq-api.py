from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def check_if_resource_exists(client, dataset_id=None, table_id=None) -> bool:
    try:
        if dataset_id is not None:
            dataset = client.get_dataset(dataset_id)
        if table_id is not None:
            table = client.get_table(table_id)
        return True
    except NotFound:
        return False


def list_datasets(client):
    datasets = list(client.list_datasets())
    project = client.project
    if datasets:
        return datasets
    else:
        print("{} project does not contain any datasets.".format(project))
        return []


def get_dataset(client, dataset_id) -> dict:
    dataset = client.get_dataset(dataset_id)
    return dataset


def create_dataset(client, dataset_id, location):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    return dataset


def modify_user_permissions_on_dataset(client, dataset_id, role, entity_type, entity_id):
    dataset = client.get_dataset(dataset_id)
    '''
    entry = bigquery.AccessEntry(
        role="READER",
        entity_type="userByEmail",
        entity_id="sample.bigquery.dev@gmail.com",
    )
    '''

    entry = bigquery.AccessEntry(
        role=role,
        entity_type=entity_type,
        entity_id=entity_id,
    )

    entries = list(dataset.access_entries)
    entries.append(entry)
    dataset.access_entries = entries

    dataset = client.update_dataset(dataset, ["access_entries"])
    return dataset


def delete_dataset(client, dataset_id):
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def list_tables_in_dataset(client, dataset_id) -> list:
    dataset = client.get_dataset(dataset_id)
    tables = list(client.list_tables(dataset))
    return tables


def get_table(client, table_id):
    table = client.get_table(table_id)
    # print("Table schema: {}".format(table.schema))
    # print("Table description: {}".format(table.description))
    # print("Table has {} rows".format(table.num_rows))
    return table


def list_rows(client, table_id):
    rows = list(client.list_rows(table_id))
    fields = table.schema[:2] # First Two Columns
    rows_iter = client.list_rows(table_id, selected_fields=fields, max_results=10)
    rows = list(rows_iter)


def create_table(client, table_id, schema):
    '''
    schema = [
        bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    ]
    '''

    table = bigquery.Table(table_id, schema=schema)
    # table.range_partitioning = bigquery.RangePartitioning(field="zipcode", range_=bigquery.PartitionRange(start=0, end=100000, interval=10),)
    table = client.create_table(table)


def load_table_from_file(client, table_id, source_file):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()
    

def load_table_from_uri(client, table_id, uri):
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()


def insert_rows(client, table_id, rows):
    table = client.get_table(table_id)  # Make an API request.
    # rows_to_insert = [(u"Phred Phlyntstone", 32), (u"Wylma Phlyntstone", 29)]
    errors = client.insert_rows(table, rows)  # Make an API request.
    if errors == []:
        print("New rows have been added.")


# Higher Rate limits
# def insert_rows(client, table_id, rows):
#     table = client.get_table(table_id)  # Make an API request.
#     # rows_to_insert = [(u"Phred Phlyntstone", 32), (u"Wylma Phlyntstone", 29)]
#     errors = client.insert_rows(table, rows, row_ids=[None] * len(rows_to_insert) )  # Make an API request.
#     if errors == []:
#         print("New rows have been added.")

def add_column_to_table(client, table_id, column_name, column_type):
    table = client.get_table(table_id)
    original_schema = table.schema
    new_schema = original_schema[:]  # Creates a copy of the schema.
    new_schema.append(bigquery.SchemaField("phone", "STRING"))
    table.schema = new_schema
    table = client.update_table(table, ["schema"])


def copy_table(client, source_table_id, destination_table_id):
    job = client.copy_table(source_table_id, destination_table_id)
    job.result()  # Wait for the job to complete.


def extract_to_gcs():
    # from google.cloud import bigquery
    # client = bigquery.Client()
    # bucket_name = 'my-bucket'
    project = "bigquery-public-data"
    dataset_id = "samples"
    table_id = "shakespeare"

    destination_uri = "gs://{}/{}".format(bucket_name, "shakespeare.csv")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )

def delete_table(client, table_id):
    client.delete_table(table_id, not_found_ok=True)

def restore_table():
    import time

    from google.cloud import bigquery

        # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Choose a table to recover.
    # table_id = "your-project.your_dataset.your_table"

    # TODO(developer): Choose a new table ID for the recovered table data.
    # recovery_table_id = "your-project.your_dataset.your_table_recovered"

    # TODO(developer): Choose an appropriate snapshot point as epoch
    # milliseconds. For this example, we choose the current time as we're about
    # to delete the table immediately afterwards.
    snapshot_epoch = int(time.time() * 1000)

    # [START_EXCLUDE]
    # Due to very short lifecycle of the table, ensure we're not picking a time
    # prior to the table creation due to time drift between backend and client.
    table = client.get_table(table_id)
    created_epoch = datetime_helpers.to_milliseconds(table.created)
    if created_epoch > snapshot_epoch:
        snapshot_epoch = created_epoch
    # [END_EXCLUDE]

    # "Accidentally" delete the table.
    client.delete_table(table_id)  # Make an API request.

    # Construct the restore-from table ID using a snapshot decorator.
    snapshot_table_id = "{}@{}".format(table_id, snapshot_epoch)

    # Construct and run a copy job.
    job = client.copy_table(
        snapshot_table_id,
        recovered_table_id,
        # Must match the source and destination tables location.
        location="US",
    )  # Make an API request.

    job.result()  # Wait for the job to complete.

    print(
        "Copied data from deleted table {} to {}".format(table_id, recovered_table_id)
    )

    # write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE

def land_csv_to_bq():
    import six

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
    )

    body = six.BytesIO(b"Washington,WA")
    client.load_table_from_file(body, table_id, job_config=job_config).result()
    previous_rows = client.get_table(table_id).num_rows
    assert previous_rows > 0

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))



