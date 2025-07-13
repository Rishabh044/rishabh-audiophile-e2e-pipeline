import boto3
import logging
from botocore.exceptions import ClientError
from io import StringIO
from typing import Optional
import psycopg2

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class AWSManager:
    def __init__(
        self, 
        aws_access_key,
        aws_secret_access_key,
        region,
        redshift_host=None,
        redshift_port="5439",
        redshift_database="audiophile_db",
        redshift_username="admin",
        redshift_password=None,
        redshift_iam_role=None
    ):
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region
        
        # Redshift connection parameters
        self.redshift_host = redshift_host
        self.redshift_port = redshift_port
        self.redshift_database = redshift_database
        self.redshift_username = redshift_username
        self.redshift_password = redshift_password
        self.redshift_iam_role = redshift_iam_role
        self.redshift_connection = None

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

    def connect_to_redshift(self) -> bool:
        """
        Establish connection to Redshift cluster.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        if not self.redshift_host or not self.redshift_password:
            log.error("Redshift host and password must be provided")
            return False
            
        try:
            self.redshift_connection = psycopg2.connect(
                host=self.redshift_host,
                port=self.redshift_port,
                database=self.redshift_database,
                user=self.redshift_username,
                password=self.redshift_password
            )
            self.redshift_connection.autocommit = True
            log.info("Successfully connected to Redshift cluster")
            return True
        except Exception as e:
            log.error(f"Failed to connect to Redshift: {str(e)}")
            return False

    def disconnect_from_redshift(self):
        """Close Redshift connection."""
        if self.redshift_connection:
            self.redshift_connection.close()
            self.redshift_connection = None
            log.info("Disconnected from Redshift cluster")

    def execute_redshift_query(self, query: str) -> bool:
        """
        Execute a query on Redshift.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            bool: True if execution successful, False otherwise
        """
        if not self.redshift_connection:
            log.error("No Redshift connection available")
            return False
            
        try:
            cursor = self.redshift_connection.cursor()
            cursor.execute(query)
            cursor.close()
            log.info("Query executed successfully")
            return True
        except Exception as e:
            log.error(f"Failed to execute query: {str(e)}")
            return False

    def copy_s3_to_redshift(self, table_name: str, s3_file_key: str, 
                           bucket_name: str, delimiter: str = ',', 
                           ignore_header: int = 1) -> bool:
        """
        Load data from S3 to Redshift table using COPY command.
        
        Args:
            table_name (str): Target Redshift table name
            s3_file_key (str): S3 file key (path within bucket)
            bucket_name (str): S3 bucket name
            delimiter (str): CSV delimiter (default: ',')
            ignore_header (int): Number of header rows to skip (default: 1)
            
        Returns:
            bool: True if COPY successful, False otherwise
        """
        if not self.redshift_connection:
            log.error("No Redshift connection available")
            return False
            
        if not self.redshift_iam_role:
            log.error("Redshift IAM role must be provided for COPY operation")
            return False
            
        try:
            s3_path = f"s3://{bucket_name}/{s3_file_key}"
            
            copy_command = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{self.redshift_iam_role}'
            DELIMITER '{delimiter}'
            IGNOREHEADER {ignore_header}
            CSV
            TIMEFORMAT 'auto'
            ACCEPTINVCHARS
            TRUNCATECOLUMNS
            """
            
            cursor = self.redshift_connection.cursor()
            log.info(f"Executing COPY command for {table_name} from {s3_path}")
            cursor.execute(copy_command)
            cursor.close()
            
            log.info(f"Successfully loaded data into {table_name}")
            return True
            
        except Exception as e:
            log.error(f"Failed to copy data to {table_name}: {str(e)}")
            return False

    def get_redshift_table_count(self, table_name: str) -> Optional[int]:
        """
        Get row count for a Redshift table.
        
        Args:
            table_name (str): Table name
            
        Returns:
            Optional[int]: Row count or None if error
        """
        if not self.redshift_connection:
            log.error("No Redshift connection available")
            return None
            
        try:
            cursor = self.redshift_connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            log.error(f"Failed to get count for {table_name}: {str(e)}")
            return None

    def load_all_data_to_redshift(self, bucket_name: str, truncate_first: bool = True) -> bool:
        """
        Load all cleaned data from S3 to Redshift tables.
        
        Args:
            bucket_name (str): S3 bucket name
            truncate_first (bool): Whether to truncate tables before loading
            
        Returns:
            bool: True if all loads successful, False otherwise
        """
        if not self.redshift_connection:
            log.error("No Redshift connection available")
            return False
            
        success = True
        
        # Define table and S3 file mappings
        load_mappings = [
            ("headphones", "headphones_cleaned.csv"),
            ("iems", "iems_cleaned.csv")
        ]
        
        for table_name, s3_file in load_mappings:
            log.info(f"Loading data into {table_name} from {s3_file}")
            
            # Truncate table if requested
            if truncate_first:
                if not self.execute_redshift_query(f"TRUNCATE TABLE {table_name}"):
                    success = False
                    continue
            
            # Load data from S3
            if not self.copy_s3_to_redshift(table_name, s3_file, bucket_name):
                success = False
                continue
            
            # Verify data was loaded
            count = self.get_redshift_table_count(table_name)
            if count is not None:
                log.info(f"Table {table_name} now has {count} rows")
            else:
                success = False
        
        return success
