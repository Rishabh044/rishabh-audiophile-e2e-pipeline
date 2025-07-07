from models.headphones_model import HeadphonesModel
from models.iems_model import IEMModel
from dotenv import load_dotenv
from typing import Optional
import os
import pandas as pd
import boto3
from pydantic import ValidationError
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

load_dotenv()


def get_files_from_s3(file_name: str, bucket_name, aws_access_key, aws_secret_access_key, region, local_path: Optional[str] = None):
     if local_path is None:
        local_path = file_name

     try:
        # Create an S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )

        # Download the file
        s3.download_file(bucket_name, file_name, local_path)
        print(f"File '{file_name}' downloaded from S3 bucket '{bucket_name}' to '{local_path}'")
        return local_path

     except Exception as e:
        print(f"Failed to download file from S3: {e}")
        raise

def create_cleaned_files(file_name, Model):
    raw_df = pd.read_csv(file_name)
    
    raw_df = raw_df.fillna('')
    
    validated_rows = []
    errors = []
    for i, row in raw_df.iterrows():
       try:
          model = Model(**row)
          validated_rows.append(model)
       except ValidationError as e:
          errors.append((i, e.errors()))
      
    log.info(f"Len of validated Rows: {len(validated_rows)}")
    log.info(f"Len of errors: {len(errors)}")
    
    return validated_rows, errors
    