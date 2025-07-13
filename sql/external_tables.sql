-- Create external schema for Redshift Spectrum to query S3 data directly
-- This allows querying S3 data without loading into Redshift tables

-- Create external schema (requires AWS Glue Data Catalog or external Hive metastore)
CREATE EXTERNAL SCHEMA IF NOT EXISTS audiophile_s3_spectrum
FROM DATA CATALOG
DATABASE 'audiophile_external_db'
IAM_ROLE '${redshift_iam_role_arn}';

-- External table for headphones data in S3
CREATE EXTERNAL TABLE IF NOT EXISTS audiophile_s3_spectrum.headphones_external (
    rank VARCHAR(5),
    value_rating VARCHAR(10),
    model VARCHAR(200),
    price_msrp INTEGER,
    signature VARCHAR(100),
    comments VARCHAR(500),
    tone_grade VARCHAR(5),
    technical_grade VARCHAR(5),
    driver_type VARCHAR(50),
    fit_cup_type VARCHAR(50),
    based_on VARCHAR(100),
    note_weight VARCHAR(100),
    ranksort DECIMAL(10,4),
    tonesort DECIMAL(10,4),
    techsort DECIMAL(10,4),
    pricesort DECIMAL(10,4)
)
STORED AS TEXTFILE
LOCATION 's3://${bucket_name}/headphones_cleaned.csv'
TABLE PROPERTIES ('has_encrypted_data'='false');

-- External table for IEMs data in S3
CREATE EXTERNAL TABLE IF NOT EXISTS audiophile_s3_spectrum.iems_external (
    rank VARCHAR(5),
    value_rating VARCHAR(10),
    model VARCHAR(200),
    price_msrp INTEGER,
    signature VARCHAR(100),
    comments VARCHAR(500),
    tone_grade VARCHAR(5),
    technical_grade VARCHAR(5),
    setup VARCHAR(100),
    status VARCHAR(50),
    ranksort DECIMAL(10,4),
    tonesort DECIMAL(10,4),
    techsort DECIMAL(10,4),
    pricesort DECIMAL(10,4)
)
STORED AS TEXTFILE
LOCATION 's3://${bucket_name}/iems_cleaned.csv'
TABLE PROPERTIES ('has_encrypted_data'='false');

-- Example queries for external tables:
-- SELECT * FROM audiophile_s3_spectrum.headphones_external WHERE rank = 'S' LIMIT 10;
-- SELECT * FROM audiophile_s3_spectrum.iems_external WHERE price_msrp < 1000 LIMIT 10;