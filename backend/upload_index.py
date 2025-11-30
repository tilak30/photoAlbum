import json
import logging
import urllib.parse
import os
import time

# --- External Library Dependency (Must be packaged in .zip file) ---
# Requires 'requests' and 'requests-aws4auth' in the deployment package.
import requests
from requests_aws4auth import AWS4Auth
# ------------------------------------------------------------------

import boto3

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables are required for configuration
OPENSEARCH_HOST = os.environ.get('OPENSEARCH_HOST')
REGION = os.environ.get('AWS_REGION')
INDEX_NAME = 'photos' # Required index name

# Initialize AWS V4 Signature authentication using the Lambda's IAM role credentials
try:
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        REGION,
        'es',  # service name for OpenSearch (formerly ElasticSearch)
        session_token=credentials.token
    )
except Exception as e:
    logger.error(f"Failed to initialize AWS4Auth: {e}. Check IAM Role.")
    awsauth = None # Will cause failure if not set

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    rekognition_client = boto3.client('rekognition')

    logger.info(f"Received S3 PUT event: {json.dumps(event)}")

    # 1. Extract event data (E1)
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        logger.info(f"Processing object: s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error extracting S3 details: {e}")
        return {'statusCode': 400, 'body': json.dumps({'message': 'Invalid event'})}

    # 2. Retrieve custom labels (x-amz-meta-customLabels)
    custom_labels = []
    try:
        # Use headObject method to retrieve only metadata, not the file content
        s3_head_object = s3_client.head_object(Bucket=bucket, Key=key)
        
        # S3 metadata keys are automatically lowercased
        metadata = s3_head_object.get('Metadata', {})
        raw_labels = metadata.get('x-amz-meta-customlabels', '')
        
        if raw_labels:
            # Clean up and split the comma-separated string (A1)
            custom_labels = [label.strip().lower() for label in raw_labels.split(',') if label.strip()]
        
        logger.info(f"Custom labels retrieved: {custom_labels}")
        created_timestamp = s3_head_object.get('LastModified', time.time()).isoformat()

    except Exception as e:
        logger.error(f"Error retrieving S3 metadata. Check IAM s3:HeadObject permissions. Error: {e}")
        created_timestamp = time.time().isoformat()
        # Continue processing even if custom labels fail

    # 3. Detect labels using Rekognition
    rekognition_labels = []
    try:
        rekognition_response = rekognition_client.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=20,
            MinConfidence=75
        )
        
        # Extract the label names and convert to lowercase for consistent searching
        rekognition_labels = [
            label['Name'].lower()
            for label in rekognition_response['Labels']
        ]
        
        logger.info(f"Rekognition labels detected: {rekognition_labels}")

    except Exception as e:
        logger.error(f"Error calling Rekognition. Check IAM permissions. Error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'message': 'Rekognition failed'})}

    # Combine all labels and remove duplicates (Case-insensitive)
    all_labels = list(set(rekognition_labels + custom_labels))
    
    # 4. Index in OpenSearch
    document = {
        'objectKey': key,
        'bucket': bucket,
        'createdTimestamp': created_timestamp,
        'labels': all_labels # This is the field we will search against
    }
    
    # The OpenSearch URL for indexing is: host/index_name/_doc/document_id
    opensearch_url = f'https://{OPENSEARCH_HOST}/{INDEX_NAME}/_doc/{key.replace("/", "_")}' # Using key as document ID
    
    if not awsauth:
        logger.error("AWS4Auth is not initialized. Cannot connect to OpenSearch.")
        return {'statusCode': 500, 'body': json.dumps({'message': 'Authentication error'})}
    
    try:
        # Send the signed request to OpenSearch
        response = requests.put( # Using PUT to ensure the document is created or updated
            opensearch_url,
            auth=awsauth,
            json=document,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code in [200, 201]:
            logger.info(f"Successfully indexed document for {key}.")
            return {'statusCode': 200, 'body': json.dumps({'message': 'Photo indexed successfully'})}
        else:
            logger.error(f"Failed to index document. Status: {response.status_code}, Response: {response.text}")
            return {'statusCode': response.status_code, 'body': json.dumps({'message': 'Failed to index photo in OpenSearch'})}

    except Exception as e:
        logger.error(f"OpenSearch connectivity error. Check network and access policy. Error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'message': 'OpenSearch indexing failed'})}