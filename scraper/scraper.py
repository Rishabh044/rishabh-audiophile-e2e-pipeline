import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from urllib.parse import urljoin, urlparse
import re
import backoff
import boto3
from botocore.exceptions import ClientError
from io import StringIO, BytesIO
import uuid


class TableScrapper:
    """
        Creates a scraper to read the tables on the crinacle site
    """
    def __init__(
        self,
        base_url: str,
    ):
        self.base_url = base_url
    
    @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=3
    )
    def get_page(self, url, delay=1): 
        time.sleep(delay)
        response = self.session.get(url, timeout=15)
        response.raise_for_status() 
        return response
    
    def find_all_tables(self, soup):
        tables = soup.find_all('table')
        print("Found {len(tables)} tables on the page")
        return tables

    def extract_table_data(self, table):
        rows = table.find_all("tr")
        data = []
        for row in rows:
            cells = row.find_all(["th", "td"])
            row_data = [cell.text.strip() for cell in cells]
            data.append(row_data)
        
        table_df = pd.DataFrame(data[1:], columns=data[0]) #Asumming that first row is headers
    
    @staticmethod
    def _validate_aws_credentials(
        self,
        aws_access_key,
        aws_secret_key,
        region='us-east-1'
    ):
        try:
            sts_client= boto3.client(
                'sts',
                aws_access_key_id=aws_access_key,
                aws_secret_key_id=aws_secret_key,
                region_name=region
            )
            response = sts_client.get_caller_identity()
            print("Credentials are valid:")
            print(response)
            return True
        except ClientError as e:
            print(f"Credentials are invalid: {e}")
            return False

    def save_to_s3(self, df, bucket_name, aws_access_key, aws_secret_key, region):
        if not self._validate_aws_credentials(aws_access_key=aws_access_key, aws_secret_key=aws_access_key, region=region):
            print("Creds invalid! Please input correct creds")
            return
        
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
        
        s3_client = session.client('s3')
        
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'df-{uuid.uuid4()}.csv',
            Body=csv_buffer.getvalue()
        )
        
        print("File saved to S3 ✅")


        



