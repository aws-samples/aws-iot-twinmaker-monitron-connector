import logging
import os
import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

DATABASE_NAME = os.environ['ATHENA_DATABASE']
TABLE_NAME = os.environ['ATHENA_TABLE']
RESULTS_LOCATION = os.environ['ATHENA_QUERY_BUCKET']

QUERY_CLIENT = boto3.client('athena')


class AthenaReader:

    def __init__(self, query_client, database_name, table_name, results_location):
        self.query_client = query_client
        self.database_name = database_name
        self.table_name = table_name
        self.results_location = results_location
        self.results = {'Properties': {}}

    def schema_query(self) -> str:
        response = self.query_client.get_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName=self.database_name,
            TableName=self.table_name
        )
        return response

    def process_query(self) -> dict:
        table_metadata = self.schema_query()
        for i in table_metadata['TableMetadata']['Columns']:
            row = {}
            row[i['Name']] = {}
            row[i['Name']]['definition'] = {}
            row[i['Name']]['definition']['DataType'] = {}
            row[i['Name']]['definition']['isTimeSeries'] = True
            row[i['Name']]['definition']['DataType']['Type'] = self.get_data_type(
                i['Type'])
            self.results['Properties'].update(row)
        return (self.results)

    def get_data_type(self, data) -> str:
        match data:
            case 'string':
                return 'STRING'
            case 'float':
                return 'DOUBLE'
            case 'double':
                return 'DOUBLE'
            case 'int':
                return 'INTEGER'
            case _:
                return 'STRING'


ATHENA_READER = AthenaReader(
    QUERY_CLIENT, DATABASE_NAME, TABLE_NAME, RESULTS_LOCATION)


def lambda_handler(event, _):
    results = ATHENA_READER.process_query()
    return results