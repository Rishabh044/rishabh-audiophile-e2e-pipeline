import boto3
import logging
from botocore.exceptions import ClientError
from io import StringIO
from typing import Optional

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class AWSManager:
    def __init__(
        self, 
        aws_access_key,
        aws_secret_access_key,
        region
    ):
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region

    def _validate_aws_credentials(self):
        try:
            sts_client = boto3.client(
                "sts",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region,
            )
            response = sts_client.get_caller_identity()
            log.info("Credentials are valid.")
            return True
        except ClientError as e:
            log.info(f"Credentials are invalid: {e}")
            return False

    def save_to_s3(
        self, df, bucket_name, file_name
    ):
        if not self._validate_aws_credentials():
            return

        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region,
        )

        s3_client = session.client("s3")

        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name, Key=f"{file_name}.csv", Body=csv_buffer.getvalue()
        )

        log.info("File saved to S3✅")
        log.info(f"File saved as {file_name}.csv")

    def get_files_from_s3(
        self, file_name: str, bucket_name, local_path: Optional[str] = None
    ):
        if local_path is None:
            local_path = file_name

        try:
            # Create an S3 client
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region,
            )

            # Download the file
            s3.download_file(bucket_name, file_name, local_path)
            print(
                f"File '{file_name}' downloaded from S3 bucket '{bucket_name}' to '{local_path}'"
            )
            return local_path

        except Exception as e:
            print(f"Failed to download file from S3: {e}")
            raise
