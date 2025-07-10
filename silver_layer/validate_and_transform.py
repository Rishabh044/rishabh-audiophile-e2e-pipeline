from models.headphones_model import HeadphonesModel
from models.iems_model import IEMModel
from scraper.aws_manager import AWSManager
from dotenv import load_dotenv
from typing import Optional, Tuple, List
import os
import pandas as pd
import boto3
from pydantic import ValidationError
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

load_dotenv()


def setup_aws_manager() -> AWSManager:
    """Initialize and return AWS manager with environment variables."""
    return AWSManager(
        aws_access_key=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        region=os.getenv("REGION"),
    )


def retrieve_files_from_s3(manager: AWSManager) -> Tuple[Optional[str], Optional[str]]:
    """Retrieve IEM and headphones files from S3."""
    bucket_name = os.getenv("BUCKET_NAME")

    iem_file_path = manager.get_files_from_s3(
        file_name="iems.csv",
        bucket_name=bucket_name,
    )
    headphones_file_path = manager.get_files_from_s3(
        file_name="headphones.csv",
        bucket_name=bucket_name,
    )

    return iem_file_path, headphones_file_path


def validate_file_retrieval(
    iem_file_path: Optional[str], headphones_file_path: Optional[str]
) -> bool:
    """Validate that both files were successfully retrieved from S3."""
    if not iem_file_path or not headphones_file_path:
        log.error("Failed to retrieve files from S3.")
        return False

    log.info(f"Retrieved IEMs file from S3: {iem_file_path}")
    log.info(f"Retrieved Headphones file from S3: {headphones_file_path}")
    return True


def create_cleaned_files(file_name: str | None, model) -> Tuple[List, List]:
    """Validates and transforms the raw data from a CSV file into a list of model instances."""
    if not file_name:
        log.error("No file name provided.")
        return [], []

    raw_df = pd.read_csv(file_name)
    raw_df = raw_df.fillna("")

    validated_rows = []
    errors = []

    for i, row in raw_df.iterrows():
        try:
            model_instance = model(**row)
            validated_rows.append(model_instance)
        except ValidationError as e:
            errors.append((i, e.errors()))

    log.info(f"Len of validated Rows: {len(validated_rows)}")
    log.info(f"Len of errors: {len(errors)}")

    return validated_rows, errors


def process_data_files(
    iem_file_path: str | None, headphones_file_path: str | None
) -> Tuple[List, List, List, List]:
    """Process both IEM and headphones data files."""
    iems, iem_errors = create_cleaned_files(iem_file_path, IEMModel)
    headphones, headphone_errors = create_cleaned_files(
        headphones_file_path, HeadphonesModel
    )

    log.info(f"Total IEMs validated: {len(iems)}")
    log.info(f"Total Headphones validated: {len(headphones)}")

    return iems, headphones, iem_errors, headphone_errors


def convert_to_dataframes(
    iems: List, headphones: List
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Convert validated model instances to DataFrames."""
    headphones_cleaned_df = pd.DataFrame([h.model_dump() for h in headphones])
    iems_cleaned_df = pd.DataFrame([i.model_dump() for i in iems])

    return headphones_cleaned_df, iems_cleaned_df


def save_cleaned_data_to_s3(
    manager: AWSManager, headphones_df: pd.DataFrame, iems_df: pd.DataFrame
) -> None:
    """Save cleaned DataFrames to S3."""
    bucket_name = os.getenv("BUCKET_NAME")

    manager.save_to_s3(
        df=headphones_df,
        bucket_name=bucket_name,
        file_name="headphones_cleaned",
    )
    manager.save_to_s3(
        df=iems_df,
        bucket_name=bucket_name,
        file_name="iems_cleaned",
    )
    log.info("Cleaned files saved to S3 successfully.")


def main():
    """Main function to orchestrate the data validation and transformation process."""
    manager = setup_aws_manager()

    iem_file_path, headphones_file_path = retrieve_files_from_s3(manager)

    if not validate_file_retrieval(iem_file_path, headphones_file_path):
        return

    iems, headphones, iem_errors, headphone_errors = process_data_files(
        iem_file_path, headphones_file_path
    )

    headphones_cleaned_df, iems_cleaned_df = convert_to_dataframes(iems, headphones)

    save_cleaned_data_to_s3(manager, headphones_cleaned_df, iems_cleaned_df)


if __name__ == "__main__":
    main()
