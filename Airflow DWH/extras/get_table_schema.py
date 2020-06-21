def get_table_schema(dataset_id: str, table_id: str) -> list:
    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bq_client.get_table(table_ref)
    schema = [{'name': field.name, 'field_type': field.field_type, 'is_nullable': field.is_nullable, 'description': field.description} for field in table.schema]
    return schema