import dateutil.parser as parser
import logging
import os
import time
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
        self.results_location = f's3://{results_location}/query_results'

    def entity_query(self, query_metric, event) -> str:
        LOGGER.info('AthenaReader entity_query')
        asset_name, position_name = event['entityId'].split(':')
        num_results = event['maxResults']

        if query_metric == 'eventpayload_assetstate_newstate':
            event_type = 'assetStateTransition'
        else:
            event_type = 'measurement'
        
        order_by = 'DESC'
        
        num_results = 1

        try:
            query = f'''
                SELECT DISTINCT "timestamp", "eventpayload_assetname", "{query_metric}" as measure
                FROM "{self.database_name}"."{self.table_name}"
                WHERE "eventtype" = '{event_type}'
                  AND "eventpayload_assetname" = '{asset_name}' 
                  AND "eventpayload_positionname" = '{position_name}'
                ORDER BY "timestamp" {order_by}
                LIMIT {num_results}
            '''
            LOGGER.info('Athena SQL Query: %s', query)
        except Exception as err:
            LOGGER.error('Exception while processing data point: %s', err)
        response = self.query_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': self.results_location}
        )
        return response['QueryExecutionId']

    def process_query(self, event) -> dict:
        # Initialize Response JSON
        jsonResponse = dict()
        jsonResponse['propertyValues'] = []

        # Loop to process multiple properties to query.
        for i in event['selectedProperties']:
            LOGGER.info('Event Processing: %s', str(i))
            loopEntityPropertyReference = {}
            loopEntityPropertyReference['entityPropertyReference'] = {}
            loopEntityPropertyReference['entityPropertyReference']['entityId'] = event['entityId']
            loopEntityPropertyReference['entityPropertyReference']['componentName'] = event['componentName']
            loopEntityPropertyReference['entityPropertyReference']['propertyName'] = i
            loopEntityPropertyReference['values'] = []
            athena_query_id = self.entity_query(i, event)
            athena_query_results = self._results_athena_query(athena_query_id)

            # Extract Data from Athena Query and format to JSON.
            heading = 0
            for row in athena_query_results:
                if heading == 0:
                    heading += 1
                    continue
                time_ISO_UTC = self._update_time_format(
                    row['Data'][0]['VarCharValue'])
                dataType = self._get_data_type(row['Data'][2]['VarCharValue'])
                valueFromQuery = {'time': time_ISO_UTC, 'value': {
                    dataType: row['Data'][2]['VarCharValue']}}
                loopEntityPropertyReference['values'].append(valueFromQuery)
            jsonResponse['propertyValues'].append(loopEntityPropertyReference)
        return jsonResponse

    def run_athena_query(self, event) -> dict:
        # Initialize Response JSON
        jsonResponse = dict()
        jsonResponse['propertyValues'] = []

        # Loop to process multiple properties to query.
        for i in event['selectedProperties']:
            LOGGER.info('Event Processing: %s', str(i))
            loopEntityPropertyReference = {}
            loopEntityPropertyReference['entityPropertyReference'] = {}
            loopEntityPropertyReference['entityPropertyReference']['entityId'] = event['entityId']
            loopEntityPropertyReference['entityPropertyReference']['componentName'] = event['componentName']
            loopEntityPropertyReference['entityPropertyReference']['propertyName'] = i
            loopEntityPropertyReference['values'] = []
            athena_query_id = self.entity_query(i, event)
            athena_query_status = self._check_athena_query(athena_query_id)
            if not athena_query_status:
                LOGGER.info(
                    'Query ID: %s returned no results. Skipping.', athena_query_id)
                continue

            athena_query_results = self._results_athena_query(athena_query_id)

            # Extract Data from Athena Query and format to JSON.
            heading = 0
            for row in athena_query_results:
                if heading == 0:
                    heading += 1
                    continue
                try:
                    time_ISO = parser.parse(row['Data'][0]['VarCharValue'])
                    time_ISO_UTC = str(time_ISO.isoformat()) + 'Z'
                except Exception as err:
                    LOGGER.error(
                        'Exception while processing timestamp: %s', err)
                    continue

                # Used to determine if value is a double or string...
                try:
                    float(row['Data'][2]['VarCharValue'])
                    dataType = 'doubleValue'
                except Exception:
                    dataType = 'stringValue'

                try:
                    valueFromQuery = {'time': time_ISO_UTC, 'value': {
                        dataType: row['Data'][2]['VarCharValue']}}
                except Exception as err:
                    LOGGER.error(
                        'Exception while processing datapoint: %s', err)
                    continue

                loopEntityPropertyReference['values'].append(valueFromQuery)
            jsonResponse['propertyValues'].append(loopEntityPropertyReference)
        return jsonResponse

    def _check_athena_query(self, query_id) -> bool:
        LOGGER.info('Checking status of Query ID: %s', query_id)
        state = 'RUNNING'
        maximum_checks = 15
        while maximum_checks > 0 and state in ['RUNNING', 'QUEUED']:
            maximum_checks -= 1
            response = self.query_client.get_query_execution(
                QueryExecutionId=query_id)
            if (
                'QueryExecution' in response
                and 'Status' in response['QueryExecution']
                and 'State' in response['QueryExecution']['Status']
            ):
                state = response['QueryExecution']['Status']['State']
                if state == 'SUCCEEDED':
                    return True
            ## time.sleep(2)
        return False

    def _results_athena_query(self, query_id):
        response = self.query_client.get_query_results(
            QueryExecutionId=query_id)
        results = response['ResultSet']['Rows']
        return results

    def _get_data_type(self, _) -> str:
        try:
            return 'doubleValue'
        except Exception:
            return 'stringValue'

    def _update_time_format(self, time_value) -> str:
        try:
            time_ISO = parser.parse(time_value)
            return (str(time_ISO.isoformat()) + 'Z')
        except Exception as err:
            LOGGER.error('Exception while processing timestamp: %s', err)


ATHENA_READER = AthenaReader(
    QUERY_CLIENT, DATABASE_NAME, TABLE_NAME, RESULTS_LOCATION)


def lambda_handler(event, _):
    results = ATHENA_READER.run_athena_query(event)
    LOGGER.info('Results:')
    LOGGER.info(results)
    return results