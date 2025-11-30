import json
import logging
import boto3
import os
import requests
from requests_aws4auth import AWS4Auth

# --- Configuration ---
# Update these placeholders with your actual values
REGION = 'us-east-1' # e.g., 'us-east-1'
ES_HOST = 'YOUR_ES_ENDPOINT_HERE'
ES_INDEX = 'photos'
LEX_BOT_ID = 'YOUR_BOT_ID' 
LEX_BOT_ALIAS_ID = 'YOUR_BOT_ALIAS_ID' 
LEX_LOCALE_ID = 'en_US'
# The Session ID can be static for non-conversational use
SESSION_ID = 'LF2SearchSession'

# --- Setup ---
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# AWS client setup (outside handler for performance)
lex_client = boto3.client('lexv2-runtime')
es_service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key, 
    credentials.secret_key, 
    REGION, 
    es_service, 
    session_token=credentials.token
)

# --- Core Logic Functions ---

def get_keywords_from_lex(query):
    """
    Calls the Lex V2 Runtime API to get the keywords (slot values) 
    from the identified intent.
    """
    try:
        response = lex_client.recognize_text(
            botId=LEX_BOT_ID,
            botAliasId=LEX_BOT_ALIAS_ID,
            localeId=LEX_LOCALE_ID,
            sessionId=SESSION_ID,
            text=query
        )
        
        # Check if the correct intent was matched (SearchIntent)
        intent = response.get('sessionState', {}).get('intent', {})
        intent_name = intent.get('name')
        slots = intent.get('slots', {})

        if intent_name == 'SearchIntent': # **Ensure this matches your intent name**
            # **IMPORTANT**: Replace 'SearchTerm' with the exact name of your slot
            slot_name = 'SearchTerm' 
            keywords = []
            
            if slots.get(slot_name):
                # Extract the interpreted value from the slot structure
                slot_value_obj = slots[slot_name]['value']
                if slot_value_obj and slot_value_obj.get('interpretedValue'):
                    # [cite_start]The assignment requires handling one or two keywords per query [cite: 41]
                    # Lex usually returns a combined string for simple queries (e.g., "cats dogs")
                    keyword_string = slot_value_obj['interpretedValue'].lower()
                    # A simple split can handle keywords separated by spaces (or commas, depending on Lex configuration)
                    keywords = [k.strip() for k in keyword_string.split() if k.strip()] 
                    
            return keywords

    except Exception as e:
        logger.error(f"Error calling Lex V2 Runtime: {e}")
        return []

def search_elasticsearch(keywords):
    """
    Searches the ElasticSearch index for photo keys matching the labels/keywords.
    """
    if not keywords:
        return []

    # Join keywords for a 'match' query on the 'labels' field
    # [cite_start]This will search for records containing any of the keywords [cite: 101]
    search_query = " ".join(keywords)
    
    query_body = {
        "query": {
            "match": {
                "labels": search_query
            }
        },
        # Only return the S3 objectKey from the source
        "_source": ["objectKey"] 
    }
    
    es_url = f'https://{ES_HOST}/{ES_INDEX}/_search'
    
    try:
        response = requests.get(
            es_url, 
            auth=awsauth, 
            headers={"Content-Type": "application/json"}, 
            data=json.dumps(query_body)
        )
        response.raise_for_status() # Raise an exception for bad status codes
        
        search_results = response.json()
        
        # [cite_start]Extract the list of S3 object keys (photo references) [cite: 26]
        photo_keys = [hit['_source']['objectKey'] for hit in search_results.get('hits', {}).get('hits', [])]
        
        return photo_keys

    except requests.exceptions.RequestException as e:
        logger.error(f"ElasticSearch query failed: {e}")
        return []

# --- Lambda Handler ---

def lambda_handler(event, context):
    
    # 1. Extract the search query 'q' from the API Gateway event
    try:
        # API Gateway passes query string parameters in event['queryStringParameters']
        query_text = event['queryStringParameters']['q']
    except (TypeError, KeyError):
        logger.warning("Query parameter 'q' not found in API Gateway event.")
        # [cite_start]Return empty list if query is missing [cite: 47]
        return {
            'statusCode': 200,
            'headers': { "Access-Control-Allow-Origin": "*" },
            'body': json.dumps([])
        }

    logger.info(f"Received query: {query_text}")

    # 2. Call Lex to get keywords
    keywords = get_keywords_from_lex(query_text)

    # 3. Search ElasticSearch with keywords
    if keywords:
        logger.info(f"Keywords from Lex: {keywords}")
        photo_keys = search_elasticsearch(keywords)
    else:
        logger.info("No keywords found by Lex. Returning empty results.")
        photo_keys = []
    
    # [cite_start]4. Return the results to the API Gateway/Frontend [cite: 44]
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin": "*"
        },
        'body': json.dumps(photo_keys)
    }
    