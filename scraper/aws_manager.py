import boto3
import logging
from botocore.exceptions import ClientError
from io import StringIO

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