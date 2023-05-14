# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from US_flight_review.items import UsFlightReviewItem

import pandas as pd
import datetime

import logging
import boto3
from botocore.exceptions import ClientError
import io
from io import StringIO # python3

import os
from dotenv import load_dotenv

load_dotenv()

class UsFlightReviewPipeline:
    def open_spider(self, spider):
        self.df_list = []
    
    def close_spider(self, spider):        
        self.df = pd.DataFrame(self.df_list, columns=spider.features)
        self.s3_upload(self.df)
        self.df.to_csv('US_flight_review_list.csv')
        
    def process_item(self, item, spider):
        item_date = datetime.datetime.strptime(item['ReviewDate'], '%d %B %Y')
        if item_date < spider.latest_date:
            spider.crawler.engine.close_spider(spider, 'Spider Ceased: Reach latest data updated')
        else:
            self.df_list.append(list(item.values()))        
        
        return item
    
    def s3_connect(self):
        
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

    def s3_upload(self, df, file_name = 'df.csv'):
        
        # I/O
        try:
            s3 = self.s3_connect()
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

    def s3_latest_date(self, spider):
        
        # Connect
        try:
            s3 = self.s3_connect()
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
                if file_name.find(".csv")!=-1 and file_name.find('_'.join(spider.company_to_crawl[spider.company_index].split())):
                    file_lst.append(file.key)
            
            # Formatting: 
            for file in file_lst:
                obj = s3.Object(bucket,file)
                data=obj.get()['Body'].read()
                df.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False))
            df = pd.concat(df, axis=0)
            df['ReviewDate'] = pd.to_datetime(df['ReviewDate'])
            latest_date = max(df['ReviewDate'])
            del df
            return latest_date
        except ClientError as e:
            logging.error(e)
            return False
            

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
