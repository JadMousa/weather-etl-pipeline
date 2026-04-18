import boto3
import json
from datetime import datetime
import logging

def save_raw_to_s3(data):
    s3 = boto3.client('s3')

    bucket_name = 'jad-data-pipeline'
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    key = f"raw/weather_{timestamp}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data)
    )

    logging.info(f"Saved raw data to S3: {key}")