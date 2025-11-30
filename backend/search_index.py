import json
import logging
import boto3
import os
import requests
from requests_aws4auth import AWS4Auth

# --- Configuration ---
# Update these placeholders with your actual values
REGION = 'us-east-1' 
ES_HOST = 'search-photos-eliyvit6bhto2sctejudtas4sm.us-east-1.es.amazonaws.com'
ES_INDEX = 'photos'
# These IDs are used to communicate with your Lex V2 Bot
LEX_BOT_ID = 'S8YGIKNQ45'         
LEX_BOT_ALIAS_ID = 'KYZRHAD1NL'    
LEX_LOCALE_ID = 'en_US'
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


def get_keywords_from_lex(query):
    """Calls Lex V2 to get keywords (slot values) from the user query."""
    try:
        response = lex_client.recognize_text(
            botId=LEX_BOT_ID,           
            botAliasId=LEX_BOT_ALIAS_ID, 
            localeId=LEX_LOCALE_ID,     
            sessionId=SESSION_ID,       
            text=query
        )
        
        logger.debug(f"Full Lex Response: {response}") 
        
        intent = response.get('sessionState', {}).get('intent', {})
        intent_name = intent.get('name')
        slots = intent.get('slots', {})
        keywords = []

        if intent_name == 'PhotoSearchIntent':
            slot_name = 'Keywords' 
            
            if slots.get(slot_name):
                slot_value_obj = slots[slot_name]['value']
                logger.debug(f"Raw Slot Value: {slot_value_obj}") 
                keyword_string = ""
                
                if slot_value_obj and slot_value_obj.get('interpretedValue'):
                    keyword_string = slot_value_obj['interpretedValue'].lower()
                    
                elif slot_value_obj and slot_value_obj.get('originalValue'):
                    keyword_string = slot_value_obj['originalValue'].lower()
                
                # Split the keyword string into a list
                keywords = [k.strip() for k in keyword_string.split() if k.strip()] 
            
            logger.info(f"Extracted keywords: {keywords}")
            return keywords
        
        else:
            logger.info(f"Lex matched a different intent: {intent_name}")
            return []

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
        response.raise_for_status() 
        
        search_results = response.json()
        
        # Extract the list of S3 object keys (e.g., "myphoto.jpg")
        photo_keys = [hit['_source']['objectKey'] for hit in search_results.get('hits', {}).get('hits', [])]
        
        return photo_keys

    except requests.exceptions.RequestException as e:
        logger.error(f"ElasticSearch query failed: {e}")
        return []

# --- Lambda Handler ---

def lambda_handler(event, context):
    
    try:
        # API Gateway passes query string parameters in event['queryStringParameters']
        query_text = event['queryStringParameters']['q']
    except (TypeError, KeyError):
        logger.warning("Query parameter 'q' not found in API Gateway event.")
        return {
            'statusCode': 200,
            'headers': { "Access-Control-Allow-Origin": "*" },
            'body': json.dumps([])
        }

    logger.info(f"Received query: {query_text}")

    # 1. Call Lex to get keywords
    keywords = get_keywords_from_lex(query_text)

    # 2. Search ElasticSearch with keywords
    if keywords:
        logger.info(f"Keywords from Lex: {keywords}")
        # THIS RETURNS AN ARRAY OF S3 OBJECT KEYS (e.g., ["family/trip.jpg", "pets/dog.png"])
        photo_keys = search_elasticsearch(keywords)
    else:
        logger.info("No keywords found by Lex. Returning empty results.")
        photo_keys = []
    
    # 3. Return the S3 object keys to the frontend
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin": "*"
        },
        'body': json.dumps(photo_keys)
    }