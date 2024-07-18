# AWS-iot-twinmaker-monitron-connector

This repo is realy to an AWS IoT blog Build Predictive Digital Twins with Amazon Monitron, AWS IoT TwinMaker and Amazon Bedrock(https://aws-blogs-prod.amazon.com/iot/optimize-industrial-operations-through-predictive-maintenance-using-amazon-monitron-aws-iot-twinmaker-and-amazon-bedrock-part-2-3d-spatial-visualization/).

TWINMAKER_SCHEMA_INITIALIZATION.py provides sample code for the Lambda function used to query the schema of the datalake. This will allow AWS IoT TwinMaker to identify the different types of data available in the data source.

- Function name: `TWINMAKER_SCHEMA_INITIALIZATION`
- Runtime: Python 3.10 or newer runtime
- Architecture: arm64, recommended
- Timeouts: 1 min 30 sec.

Configure the Lambda function environment variables with the datalake connection properties:

| Key | Value |
| --- | --- |
| athenaDatabase | `<YOUR_DATA_CATALOG_DATABASE_NAME>` |
| athenaTable | `<YOUR_DATA_CATALOG_TABLE_NAME>` |
| athenaQueryTemp | `s3://<YOUR_S3_BUCKET_NAME>/AthenaQuery/` |


TWINMAKER_DATA_READER.py provides sample code for the Lambda function that will be used to query data from the datalake based on the request it receives from AWS IoT TwinMaker.

- Lambda function name: `TWINMAKER_DATA_READER`
- Runtime: Python 3.10 or newer runtime
- Architecture: arm64, recommended
- Timeouts: 1 min 30 sec.

Configure the Lambda function environment variables with the datalake connection properties:

| Key | Value |
| --- | --- |
| athenaDatabase | `<YOUR_DATA_CATALOG_DATABASE_NAME>` |
| athenaTable | `<YOUR_DATA_CATALOG_TABLE_NAME>` |
| athenaQueryTemp | `s3://<YOUR_S3_BUCKET_NAME>/AthenaQuery/` |

LAMBDA_IAM_ROLE.json Create an IAM role that can be assumed by Lambda. The same IAM role will be used by both Lambda functions. Add this json file as IAM policy to the role.

TWINMAKER_EXECUTION_ROLE.json configures TwinMaker execution Role to invoke Lambda functions to query the S3 Data via Athena.

TWINMAKER_COMPONENT_CREATE.json, this sample JSON will be used to create a component which will allow TwinMaker to access and query data from the datalake. Follow these steps to create the component:

1. Open your AWS IoT TwinMaker workspace
2. Click on **Component Types** in the menu on the left side of the screen
3. Click the **Create Component Type** button
4. Copy the JSON below and paste into the **Request** portion of the screen; this will auto-complete all the fields in this screen



## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

