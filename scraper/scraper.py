import requests
from requests import Response
from bs4 import BeautifulSoup
from bs4.element import ResultSet
import pandas as pd
import time
import os
import re
import backoff
import boto3
from botocore.exceptions import ClientError
from io import StringIO
import logging
from dotenv import load_dotenv
from aws_manager import AWSManager


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


load_dotenv()


BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
REGION = os.getenv("REGION")


class TableScrapper:
    """
    Creates a scraper to read the tables on the crinacle site
    """

    def __init__(
        self,
        base_url: str,
    ):
        self.base_url = base_url
        self.session = requests.Session()

    @backoff.on_exception(
        backoff.expo, requests.exceptions.RequestException, max_tries=3
    )
    def get_page(self, url: str, delay: int = 1) -> Response:
        """
        Fetches the page with backoff in case of RequestExceptions

        Args:
            url (str): URL of site to be fetched
            delay (int, optional): delay between the consecutive requests.

        Returns:
            Response: returns response object
        """
        time.sleep(delay)
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        log.info(f"[get_page] response status code: {response.status_code}")
        return response

    def find_all_tables(self, soup: BeautifulSoup) -> ResultSet:
        tables = soup.find_all("table")
        log.info(f"[find_all_tables] Found {len(tables)} tables on the page")
        return tables

    def extract_table_data(self, table) -> pd.DataFrame:
        rows = table.find_all("tr")
        data = []
        for row in rows:
            cells = row.find_all(["th", "td"])
            row_data = [cell.text.strip() for cell in cells]
            data.append(row_data)

        table_df = pd.DataFrame(
            data[1:], columns=data[0]
        )  # Assumming that first row is headers
        return table_df


def main():
    CRINACLE_URL = "https://crinacle.com/rankings/iems/"  # URL for iems, ofc :)
    scraper = TableScrapper(CRINACLE_URL)
    manager = AWSManager(AWS_ACCESS_KEY, AWS_SECRET_KEY, REGION)
    page = scraper.get_page(CRINACLE_URL)

    soup = BeautifulSoup(page.text, "html.parser")
    tables = scraper.find_all_tables(soup)

    table_data_df = scraper.extract_table_data(
        tables[0]
    )  # we have 2 df but they are same

    manager.save_to_s3(
        table_data_df,
        bucket_name=BUCKET_NAME,
        file_name="iems",
    )


if __name__ == "__main__":
    main()
