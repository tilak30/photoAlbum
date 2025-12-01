import json
import boto3
import urllib3
import time
import urllib.parse
import os

# --- CONFIGURATION ---
OS_ENDPOINT = os.environ.get('OS_ENDPOINT', '')
OS_USER = os.environ.get('OS_USER', '')
OS_PASS = os.environ.get('OS_PASS', '')
BOT_ID = os.environ.get('BOT_ID', '')
BOT_ALIAS_ID = os.environ.get('BOT_ALIAS_ID', '')
TARGET_BUCKET_NAME = os.environ.get('TARGET_BUCKET_NAME', '')
# ---------------------

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))
    
    # ROUTER: Determine if we are Indexing (S3 Event) or Searching (API Gateway Event)
    if 'Records' in event:
        return handle_indexing(event)
    elif 'queryStringParameters' in event:
        return handle_search(event)
    else:
        return {'statusCode': 400, 'body': json.dumps('Unknown Event Type')}

# --- LOGIC 1: INDEXING (LF1) ---
def handle_indexing(event):
    http = urllib3.PoolManager()
    s3 = boto3.client('s3')
    rekognition = boto3.client('rekognition')
    
    try:
        record = event['Records'][0]['s3']
        bucket = record['bucket']['name']
        key = urllib.parse.unquote_plus(record['object']['key'])
        
        print(f"Indexing Image: {key} from {bucket}")
        
        # 1. Detect Labels
        rekog = rekognition.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': key}}, MaxLabels=10)
        labels = [l['Name'] for l in rekog['Labels']]
        
        # 2. Get Metadata
        meta = s3.head_object(Bucket=bucket, Key=key)
        custom = meta.get('Metadata', {}).get('customlabels', '')
        if custom: labels.extend([x.strip() for x in custom.split(',')])
        
        # 3. Index to OpenSearch
        doc = {"objectKey": key, "bucket": bucket, "createdTimestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()), "labels": labels}
        
        endpoint = OS_ENDPOINT.replace('https://', '').replace('/', '')
        url = f"https://{endpoint}/photos/_doc"
        auth = urllib3.make_headers(basic_auth=f"{OS_USER}:{OS_PASS}")
        headers = {'Content-Type': 'application/json'}
        headers.update(auth)
        
        http.request('POST', url, body=json.dumps(doc).encode('utf-8'), headers=headers)
        return {'statusCode': 200, 'body': 'Indexed'}
    except Exception as e:
        print(f"Indexing Error: {e}")
        return {'statusCode': 500, 'body': str(e)}

# --- LOGIC 2: SEARCH (LF2) ---
def handle_search(event):
    http = urllib3.PoolManager()
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,GET'
    }
    
    try:
        q = event.get('queryStringParameters', {}).get('q', 'dogs')
        print(f"Search Query: {q}")
        
        # 1. Lex Disambiguation
        client = boto3.client('lexv2-runtime')
        lex_resp = client.recognize_text(botId=BOT_ID, botAliasId=BOT_ALIAS_ID, localeId='en_US', sessionId='test', text=q)
        slots = lex_resp.get('sessionState', {}).get('intent', {}).get('slots', {})
        keyword = q
        if slots and slots.get('keywords') and slots['keywords'].get('value'):
            keyword = slots['keywords']['value']['originalValue']
            
        # 2. Search OpenSearch
        endpoint = OS_ENDPOINT.replace('https://', '').replace('/', '')
        url = f"https://{endpoint}/photos/_search?q=labels:{keyword}"
        auth = urllib3.make_headers(basic_auth=f"{OS_USER}:{OS_PASS}")
        
        resp = http.request('GET', url, headers=auth)
        data = json.loads(resp.data.decode('utf-8'))
        
        results = []
        unique_urls = set()
        
        if 'hits' in data:
            for hit in data['hits']['hits']:
                src = hit['_source']
                bucket = src.get('bucket')
                key = src.get('objectKey')
                
                # Filter Logic
                if TARGET_BUCKET_NAME and bucket != TARGET_BUCKET_NAME:
                    continue
                
                img_url = f"https://{bucket}.s3.amazonaws.com/{key}"
                if img_url in unique_urls:
                    continue
                
                unique_urls.add(img_url)
                results.append({"url": img_url, "labels": src['labels']})
        
        return {'statusCode': 200, 'headers': headers, 'body': json.dumps(results)}
    except Exception as e:
        print(f"Search Error: {e}")
        return {'statusCode': 500, 'headers': headers, 'body': json.dumps({"error": str(e)})}