# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from US_flight_review.items import UsFlightReviewItem

import pandas as pd

import logging
import boto3
from botocore.exceptions import ClientError
from io import StringIO # python3

import os
from dotenv import load_dotenv

load_dotenv()

class UsFlightReviewPipeline:
    def open_spider(self, spider):
        self.df_list = []
    
    def close_spider(self, spider):        
        self.df = pd.DataFrame(self.df_list, columns=spider.features)
        self.df.to_csv('US_flight_review_list.csv')
        s3 = self.connect_s3()
        
    def process_item(self, item, spider):
        self.df_list.append(list(item.values()))
        return item
    
    def s3_connect(self):
        s3 = boto3.resource(
        service_name = 's3',
        region_name = 'us-east-2',
        aws_access_key_id = os.getenv('AWS_KEY'),
        aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
        )
        # Print out bucket names
        for bucket in s3.buckets.all():
            print(bucket.name)
        print(os.getenv('AWS_KEY'))
        print(os.getenv('AWS_SECRET_KEY'))
        return s3

    def s3_upload(self, df, file_name = 'df.csv'):
        
        # Connect
        try:
            s3 = self.s3_connect()
        except:
            logging.error(e)
        
        # Upload
        try:
            bucket = 'cuma-p1' # already created on S3
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3.Object(bucket, file_name).put(Body=csv_buffer.getvalue())
        
        except ClientError as e:
            logging.error(e)
            return False

        return True
            

class UsCompanyPipeline:
    def open_spider(self, spider):
        self.df_list = []
    
    def close_spider(self, spider):
        self.df = pd.DataFrame(self.df_list, columns=['name', 'iata_code', 'icao_code', 
                                                      'callsign', 'founded', 'commenced_operations',
                                                      'ceased_operations','notes'])
        self.df.to_csv('US_company_list.csv')
        
        
    def process_item(self, item, spider):
        print(item)
        return
