import json
from bs4 import BeautifulSoup
import billboard
import boto3
import re

from datetime import datetime

s3_client = boto3.client("s3")
S3_BUCKET = 'pvdsongbucket'
def get_top_100_billboard(date):
    chart = billboard.ChartData(f"hot-100/{date}/", year=None, fetch=True, timeout=30)
    print("___DATA CRAWLED")
    top100 = []
    for i, entry in enumerate(chart):
        top100.append({
            "date": date,
            "rank": i+1,
            "track_name": entry.title,
            "artist_name": split_artists(entry.artist)[0] if split_artists(entry.artist) else "None"
        })
    print("___DATA CRAWLED (1)")
    return top100
    
def split_artists(artist_str):
    if not artist_str:
        return []
    
    # Convert word to &
    cleaned = re.sub(
        r'\b(feat|featuring|ft|with|and)\b', '&', artist_str, flags=re.IGNORECASE
    )
    cleaned = re.sub(r'[.,]', '', cleaned)
    
    parts = re.split(r'\s*&', cleaned)
    
    return [part.strip().title() for part in parts if part.strip()]

def write_to_local(data, localpath):
    file_name = localpath
    with open(file_name, "w") as file:
        # for item in data:
        json.dump(data, file, indent=2)
    return file_name

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    today = datetime.today().strftime('%Y-%m-%d')
    print("___Today billboard collecting___")
    try:
        chart = get_top_100_billboard(today)
    except Exception as e:
        print(f"Failed to get chart data: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to get chart data')
        }
    print("___Top 100 collected___")
    localpath = "/tmp/top100.json"
    datalakepath = f"top100tracks/billboard_top100_{today}.json"
    write_to_local(chart, localpath)
    print("___Writed file to path: " + localpath + "___")
    s3_client.upload_file(localpath, S3_BUCKET, datalakepath)
    print("___Uploaded file to datalake___")
