import os
import pandas as pd
from google.cloud.exceptions import NotFound
from google.cloud import bigquery

class BigQuery:
    def __init__(self, key_path:str, project_id:str):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
        self.project_id = project_id
        self.client = bigquery.Client(project=self.project_id)

    def execute_query(self, query:str):
        """
        Execute a bigquery query
            query: the query you want to execute
        """
        query_job = self.client.query(query)
        query_job.result()

    def create_table(self, dataset_id:str, table_id:str, schema:dict, partition_field:str=None):
        """
        Create a table in bigquery
            dataset_id: bigquery dataset id
            table_id: the table id you want to create
            schema: table schema
            partition_field: partition field
        """
        table_ref = bigquery.TableReference.from_string(f'{self.project_id}.{dataset_id}.{table_id}')
        table = bigquery.Table(table_ref, schema=schema)
        if partition_field is not None:
            table.time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.DAY,
                field = partition_field)
        self.client.create_table(table)
        print(f"Created table {self.project_id}.{dataset_id}.{table_id}")

    def upload_table(self, data:pd.DataFrame, dataset_id:str, table_id:str, schema:dict, method:str='append'):
        """
        Upload a table to bigquery(replace)
            data: the table you want to upload
            dataset id: bigquery dataset id
            table id: the table id you want to upload
            schems: table schema
            schema = [
            {'name': 'id', 'type': 'integer'},
            {'name': 'pk_type', 'type': 'integer'},
            {'name': 'pk_type1', 'type': 'string'},
            {'name': 'pk_id', 'type': 'STRING'},
            {'name': 'zone', 'type': 'integer'},
            {'name': 'pk_road', 'type': 'STRING'},
            {'name': 'geometry', 'type': 'GEOGRAPHY'}]
            method: append or replace
        """
        if method == 'replace':
            data.to_gbq(f'{dataset_id}.{table_id}', self.project_id, if_exists='replace', table_schema=schema)
        else:
            data.to_gbq(f'{dataset_id}.{table_id}', self.project_id, if_exists='append', table_schema=schema)

    def read_table(self, query:str):
        """
        Read a table from bigquery
            query: use query to get data
        """
        data = pd.read_gbq(query, project_id=self.project_id, dialect='standard')
        return data


    def convert_schema(self, bq_schema:bigquery.SchemaField):
        """
        Convert bigquery.SchemaField to json schema
            bq_schema: bigquery schemafield
        """
        result = []
        for i in range(len(bq_schema)):
            subset = dict()
            subset['name'] = bq_schema[i].name
            subset['type'] = bq_schema[i].field_type
            subset['mode'] = bq_schema[i].mode
            subset['description'] = bq_schema[i].description
            result.append(subset)
        return result

    def check_table_exist(self, dataset_id, table_id):
        """
        Check whether a table has existed in the bigquery
            dataset_id: dataset id
            table_id: the table id you want to check
        """
        table_ref = self.client.dataset(dataset_id, project=self.project_id).table(table_id)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound as e:
            print(f'{self.project_id}.{dataset_id}.{table_id} NotFound')
            return False
