# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is an end-to-end data pipeline for audiophile equipment rankings that scrapes, validates, and processes data from Crinacle's rankings website. The pipeline consists of three main layers:

### 1. Data Scraping Layer (`scraper/`)
- **TableScrapper**: Web scraper that fetches ranking tables from Crinacle's website using BeautifulSoup
- **AWSManager**: Handles S3 operations for data storage and retrieval
- **scrape_data.py**: Main orchestration script that coordinates scraping and S3 uploads

### 2. Data Models Layer (`models/`)
- **HeadphonesModel**: Pydantic model for headphones data validation with fields like rank, price, tone_grade, technical_grade, driver_type, etc.
- **IEMModel**: Pydantic model for In-Ear Monitor data validation with fields like rank, price, setup, status, etc.
- Both models include custom field validators for rank grades (S, A+, A, B+, etc.), star ratings, and price parsing

### 3. Silver Layer (`silver_layer/`)
- **validate_and_transform.py**: Downloads raw CSV files from S3, validates data against Pydantic models, handles transformation errors, and loads clean data to Redshift

### 4. Infrastructure (`terraform/`)
- **s3.tf**: AWS S3 bucket configuration with versioning
- **redshift.tf**: Amazon Redshift cluster, VPC, security groups, and IAM roles
- **provider.tf**: AWS provider configuration
- **variables.tf**: Terraform variable definitions including Redshift parameters
- **output.tf**: Terraform output definitions including Redshift connection details

### 5. Database Layer (`sql/`)
- **create_headphones_table.sql**: DDL for headphones table with constraints and indexes
- **create_iems_table.sql**: DDL for IEMs table with constraints and indexes
- **external_tables.sql**: Redshift Spectrum external tables for direct S3 querying

### 6. Data Loading (`redshift_loader.py`)
- Standalone script for loading data from S3 to Redshift using COPY commands
- Handles table creation, data validation, and connection management

## Key Data Flow

1. **Scraping**: `scrape_data.py` uses `TableScrapper` to fetch data from Crinacle's headphones and IEMs ranking pages
2. **Storage**: Raw data is uploaded to S3 as CSV files via `AWSManager`
3. **Validation**: `validate_and_transform.py` downloads CSVs from S3 and validates against Pydantic models
4. **Data Loading**: Clean data is uploaded back to S3 and automatically loaded into Redshift tables
5. **Querying**: Data can be queried directly in Redshift or via Spectrum from S3
6. **Error Handling**: Validation errors are logged and tracked separately from successfully validated records

## Development Commands

### Python Environment
```bash
# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies (inferred from imports)
pip install boto3 pandas pydantic beautifulsoup4 requests python-dotenv backoff numpy psycopg2-binary
```

### Running the Pipeline
```bash
# Run the scraper to fetch and upload data
python scraper/scrape_data.py

# Run data validation and transformation (now includes Redshift loading)
python silver_layer/validate_and_transform.py

# Run standalone Redshift loader (optional)
python redshift_loader.py
```

### Infrastructure
```bash
# Initialize and apply Terraform configuration
cd terraform/
terraform init
terraform plan
terraform apply

# Note: You'll need to provide redshift_master_password variable
terraform apply -var="redshift_master_password=your_secure_password"
```

### Redshift Operations
```bash
# Connect to Redshift cluster (after deployment)
psql -h your_cluster_endpoint -U admin -d audiophile_db -p 5439

# Example queries
SELECT COUNT(*) FROM headphones;
SELECT * FROM iems WHERE rank = 'S' LIMIT 10;
SELECT model, price_msrp, rank FROM headphones WHERE price_msrp < 500 ORDER BY rank;
```

## Environment Configuration

The project uses environment variables loaded via `.env` file:

### AWS S3 Configuration
- `AWS_ACCESS_KEY`: AWS access key for S3 operations
- `AWS_SECRET_KEY`: AWS secret key for S3 operations
- `REGION`: AWS region for S3 bucket
- `BUCKET_NAME`: S3 bucket name for data storage

### Redshift Configuration
- `REDSHIFT_HOST`: Redshift cluster endpoint (from Terraform output)
- `REDSHIFT_PORT`: Redshift port (default: 5439)
- `REDSHIFT_DATABASE`: Database name (default: audiophile_db)
- `REDSHIFT_USERNAME`: Database username (default: admin)
- `REDSHIFT_PASSWORD`: Database password (must match Terraform variable)
- `REDSHIFT_IAM_ROLE`: IAM role ARN for S3 access (from Terraform output)

See `.env.example` for template configuration.

## Important Implementation Details

### Data Validation
- Both models validate rank fields against specific grade values (S, S-, A+, A, A-, B+, B, B-, C+, C, C-, D+, D, D-, E, F)
- Price validation handles discontinued products, parenthetical text, and mathematical expressions using `eval()`
- Star ratings are validated to contain only '★' characters

### Error Handling
- `TableScrapper` uses exponential backoff for request retries
- AWS operations include credential validation
- Validation errors are collected and logged separately from successful records

### Security Note
- The price validation in both models uses `eval()` which could be a security risk - consider using `ast.literal_eval()` or a safer expression parser for production use

## File Structure Context

- `datasets/`: Contains CSV files with ranking data
- `venv/`: Python virtual environment
- Root CSV files (`headphones.csv`, `iems.csv`): Downloaded data files from S3
- `.gitignore`: Excludes sensitive files like `.env`, terraform state, and Python cache files