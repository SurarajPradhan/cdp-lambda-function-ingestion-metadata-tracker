from datetime import datetime
import json
import boto3

dd = boto3.resource('dynamodb')
table = dd.Table('cdp_dev_dic_dms_task_tracker')


def lambda_handler(event, context):
    try:
        print(f'event : {event}')
        print(f'context : {context}')

        # Extract required parameters from the event
        # IngestionDate=event.get('IngestionDate')
        # JobName = event.get('JobName')
        # dmsJobName = event.get('DMSJobName')
        # JobRunID = event.get('JobRunID')
        # schemaName = event.get('schemaName')
        # tableName = event.get('tableName')
        #
        # cdi_sf_execution_id = event.get('cdi_sf_execution_id')
        # cdi_dms_task_startTime = event.get('cdi_dms_task_startTime')
        # cdi_dms_task_name = event.get('cdi_dms_task_name')

        # if not (IngestionDate and JobName and dmsJobName and JobRunID and schemaName and tableName and
        #        cdi_sf_execution_id and cdi_dms_task_startTime and cdi_dms_task_name):
        #    raise ValueError("One or more required event parameters are missing.")

        # start_timestamp = str(datetime.now())
        # table.put_item(
        #    Item={
        #        'dms_task_name': cdi_dms_task_name,
        #        'sf_execution_id': cdi_sf_execution_id,
        #        'dms_task_ingestion_date': IngestionDate,
        #        'job_state': 'STARTED (DMS Task)',
        #        'start_timestamp': cdi_dms_task_startTime,
        #        'update_timestamp': 'null',
        #        'job_message': 'Job Triggered by StepFunction',
        #        'schema_name': schemaName,
        #        'table_name': tableName,
        #        'dms_post_start_run_request' : {'M': {}}
        #    }
        # )

        return {
            'statusCode': 200,
            'body': json.dumps('Data_Ingestion_Tracker_Metadata is updated!')
        }
    except Exception as e:
        error_message = f"Error: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }