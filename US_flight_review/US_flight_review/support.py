import pandas as pd
import datetime

import logging
import boto3
from botocore.exceptions import ClientError
import io
from io import StringIO # python3

import re

import os
from dotenv import load_dotenv

load_dotenv()

def s3_connect():
    
    # I/O: Connection set-up
    s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-2',
    aws_access_key_id = os.getenv('AWS_KEY'),
    aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
    )
    
    # Print out bucket names
    for bucket in s3.buckets.all():
        print(bucket.name)
    #print(os.getenv('AWS_KEY'))
    #print(os.getenv('AWS_SECRET_KEY'))
    
    return s3

def s3_upload(df, file_name = 'df.csv'):
    
    # I/O
    try:
        s3 = s3_connect()
    except:
        logging.error(e)
    
    try:
        bucket = 'cuma-p1' # already created on S3
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3.Object(bucket, 'us-flight-review/'+file_name).put(Body=csv_buffer.getvalue())
    except ClientError as e:
        logging.error(e)
        return False

    return True

def s3_snapshot(spider):
    
    # Connect
    try:
        s3 = s3_connect()
        
    except:
        logging.error(e)
        
    # Upload
    try:
        file_lst = []
        df = []
        bucket = 'cuma-p1' # already created on S3
        review_bucket = s3.Bucket(bucket)
        
        # I/O: Read/Filter the datasheet
        for file in review_bucket.objects.all():
            file_name=file.key
            if file_name.find(".csv")!=-1 and file_name.find('_'.join(spider.company_to_crawl.split())):
                file_lst.append(file.key)
        
        # Formatting: 
        for file in file_lst:
            obj = s3.Object(bucket,file)
            data=obj.get()['Body'].read()
            df.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False))
        df = pd.concat(df, axis=0)
        df['ReviewDate'] = df['ReviewDate'].apply(remove_ordinal_suffix)
        df['ReviewDate'] = pd.to_datetime(df['ReviewDate'])
        return df
    
    except ClientError as e:
        logging.error(e)
        return False

    
    

# define a function to remove ordinal suffix from a string
def remove_ordinal_suffix(date_string):
    clean_date_string = re.sub(r'\b\d+(st|nd|rd|th)\b', lambda m: m.group().rstrip('stndrdth'), date_string)
    return clean_date_string