import psycopg2
import os
import logging
from typing import Optional, List
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class RedshiftLoader:
    """
    Class to handle loading data from S3 to Redshift using COPY commands.
    """
    
    def __init__(self):
        """Initialize Redshift connection parameters from environment variables."""
        self.host = os.getenv("REDSHIFT_HOST")
        self.port = os.getenv("REDSHIFT_PORT", "5439")
        self.database = os.getenv("REDSHIFT_DATABASE", "audiophile_db")
        self.username = os.getenv("REDSHIFT_USERNAME", "admin")
        self.password = os.getenv("REDSHIFT_PASSWORD")
        self.iam_role = os.getenv("REDSHIFT_IAM_ROLE")
        self.bucket_name = os.getenv("BUCKET_NAME")
        self.connection = None
    
    def connect(self) -> bool:
        """
        Establish connection to Redshift cluster.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            self.connection.autocommit = True
            logger.info("Successfully connected to Redshift cluster")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {str(e)}")
            return False
    
    def disconnect(self):
        """Close Redshift connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Redshift cluster")
    
    def execute_sql_file(self, sql_file_path: str) -> bool:
        """
        Execute SQL commands from a file.
        
        Args:
            sql_file_path (str): Path to SQL file
            
        Returns:
            bool: True if execution successful, False otherwise
        """
        try:
            with open(sql_file_path, 'r') as file:
                sql_content = file.read()
            
            # Split SQL content by semicolons to execute individual statements
            sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            cursor = self.connection.cursor()
            
            for statement in sql_statements:
                if statement:
                    logger.info(f"Executing SQL: {statement[:100]}...")
                    cursor.execute(statement)
            
            cursor.close()
            logger.info(f"Successfully executed SQL file: {sql_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to execute SQL file {sql_file_path}: {str(e)}")
            return False
    
    def create_tables(self) -> bool:
        """
        Create headphones and iems tables using DDL scripts.
        
        Returns:
            bool: True if table creation successful, False otherwise
        """
        table_files = [
            "sql/create_headphones_table.sql",
            "sql/create_iems_table.sql"
        ]
        
        for table_file in table_files:
            if not self.execute_sql_file(table_file):
                return False
        
        logger.info("All tables created successfully")
        return True
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate a table before loading new data.
        
        Args:
            table_name (str): Name of the table to truncate
            
        Returns:
            bool: True if truncation successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.close()
            logger.info(f"Successfully truncated table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to truncate table {table_name}: {str(e)}")
            return False
    
    def copy_from_s3(self, table_name: str, s3_file_key: str, 
                     delimiter: str = ',', ignore_header: int = 1) -> bool:
        """
        Load data from S3 to Redshift table using COPY command.
        
        Args:
            table_name (str): Target Redshift table name
            s3_file_key (str): S3 file key (path within bucket)
            delimiter (str): CSV delimiter (default: ',')
            ignore_header (int): Number of header rows to skip (default: 1)
            
        Returns:
            bool: True if COPY successful, False otherwise
        """
        try:
            s3_path = f"s3://{self.bucket_name}/{s3_file_key}"
            
            copy_command = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{self.iam_role}'
            DELIMITER '{delimiter}'
            IGNOREHEADER {ignore_header}
            CSV
            TIMEFORMAT 'auto'
            ACCEPTINVCHARS
            TRUNCATECOLUMNS
            """
            
            cursor = self.connection.cursor()
            logger.info(f"Executing COPY command for {table_name} from {s3_path}")
            cursor.execute(copy_command)
            cursor.close()
            
            logger.info(f"Successfully loaded data into {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy data to {table_name}: {str(e)}")
            return False
    
    def get_table_count(self, table_name: str) -> Optional[int]:
        """
        Get row count for a table.
        
        Args:
            table_name (str): Table name
            
        Returns:
            Optional[int]: Row count or None if error
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Failed to get count for {table_name}: {str(e)}")
            return None
    
    def load_all_data(self, truncate_first: bool = True) -> bool:
        """
        Load all cleaned data from S3 to Redshift tables.
        
        Args:
            truncate_first (bool): Whether to truncate tables before loading
            
        Returns:
            bool: True if all loads successful, False otherwise
        """
        success = True
        
        # Define table and S3 file mappings
        load_mappings = [
            ("headphones", "headphones_cleaned.csv"),
            ("iems", "iems_cleaned.csv")
        ]
        
        for table_name, s3_file in load_mappings:
            logger.info(f"Loading data into {table_name} from {s3_file}")
            
            # Truncate table if requested
            if truncate_first:
                if not self.truncate_table(table_name):
                    success = False
                    continue
            
            # Load data from S3
            if not self.copy_from_s3(table_name, s3_file):
                success = False
                continue
            
            # Verify data was loaded
            count = self.get_table_count(table_name)
            if count is not None:
                logger.info(f"Table {table_name} now has {count} rows")
            else:
                success = False
        
        return success
    
    def validate_connection_params(self) -> bool:
        """
        Validate that all required connection parameters are available.
        
        Returns:
            bool: True if all parameters available, False otherwise
        """
        required_params = {
            "REDSHIFT_HOST": self.host,
            "REDSHIFT_PASSWORD": self.password,
            "REDSHIFT_IAM_ROLE": self.iam_role,
            "BUCKET_NAME": self.bucket_name
        }
        
        missing_params = [param for param, value in required_params.items() if not value]
        
        if missing_params:
            logger.error(f"Missing required environment variables: {missing_params}")
            return False
        
        return True


def main():
    """Main function to execute the data loading process."""
    loader = RedshiftLoader()
    
    # Validate connection parameters
    if not loader.validate_connection_params():
        logger.error("Cannot proceed without required connection parameters")
        return False
    
    # Connect to Redshift
    if not loader.connect():
        logger.error("Cannot proceed without Redshift connection")
        return False
    
    try:
        # Create tables if they don't exist
        logger.info("Creating tables...")
        if not loader.create_tables():
            logger.error("Failed to create tables")
            return False
        
        # Load all data
        logger.info("Loading data from S3 to Redshift...")
        if not loader.load_all_data():
            logger.error("Failed to load all data")
            return False
        
        logger.info("Data loading completed successfully!")
        return True
        
    finally:
        loader.disconnect()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)