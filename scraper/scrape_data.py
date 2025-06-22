from table_scraper import TableScrapper
from aws_manager import AWSManager
from bs4 import BeautifulSoup
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def upload_to_s3(url: str, aws_access_key, aws_secret_access_key, region, bucket_name, file_name):
    scraper = TableScrapper(url)
    manager = AWSManager(aws_access_key, aws_secret_access_key, region)

    page = scraper.get_page(url)
    soup = BeautifulSoup(page.text, "html.parser")
    tables = scraper.find_all_tables(soup)

    table_data_df = scraper.extract_table_data(
        tables[0]
    )  # we have 2 df but they are same

    manager.save_to_s3(
        table_data_df,
        bucket_name=bucket_name,
        file_name=file_name,
    )
    log.info(f"[upload_to_s3] Uploaded {file_name}")

def main():
    iems_url = "https://crinacle.com/rankings/iems/"
    headphones_url = "https://crinacle.com/rankings/headphones/"

    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_KEY")
    region = os.getenv("REGION")
    bucket_name = os.getenv("BUCKET_NAME")

    upload_to_s3(iems_url, aws_access_key=aws_access_key, aws_secret_access_key=aws_secret_access_key, region=region, bucket_name=bucket_name, file_name="iems")
    upload_to_s3(headphones_url, aws_access_key=aws_access_key, aws_secret_access_key=aws_secret_access_key, region=region, bucket_name=bucket_name, file_name="headphones")

if __name__ == "__main__":
    main()
