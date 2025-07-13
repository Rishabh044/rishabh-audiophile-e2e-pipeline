# S3 to Redshift Integration Guide

This guide explains how to set up and use the S3 to Redshift integration for querying your audiophile data.

## Quick Start

### 1. Deploy Infrastructure

```bash
cd terraform/
terraform init
terraform plan
terraform apply -var="redshift_master_password=your_secure_password"
```

### 2. Configure Environment

Copy `.env.example` to `.env` and update with your values:
```bash
cp .env.example .env
# Edit .env with your Redshift connection details
```

### 3. Run the Pipeline

```bash
# This will now automatically load data to Redshift
python silver_layer/validate_and_transform.py
```

## Connection Methods

### Method 1: COPY Command (Recommended)
- **What**: Bulk loads CSV data from S3 into Redshift tables
- **When**: Best for analytical queries requiring fast performance
- **How**: Automatically handled by the pipeline

### Method 2: Redshift Spectrum
- **What**: Query S3 data directly without loading into Redshift
- **When**: Good for ad-hoc analysis or one-time queries
- **How**: Use external tables (see `sql/external_tables.sql`)

## Database Schema

### Tables Created:
- `headphones`: Full headphones ranking data with indexes
- `iems`: Full IEMs ranking data with indexes

### Key Indexes:
- `rank`, `price_msrp`, `model` for fast filtering
- `tone_grade`, `technical_grade` for performance queries

## Example Queries

### Find Top Ranked Products Under $500
```sql
SELECT model, price_msrp, rank, tone_grade 
FROM headphones 
WHERE price_msrp < 500 AND rank IN ('S', 'A+', 'A')
ORDER BY rank, price_msrp;
```

### Compare Value Ratings Across Price Ranges
```sql
SELECT 
    CASE 
        WHEN price_msrp < 200 THEN 'Budget'
        WHEN price_msrp < 500 THEN 'Mid-range'
        ELSE 'High-end'
    END as price_category,
    AVG(LENGTH(value_rating)) as avg_stars,
    COUNT(*) as product_count
FROM iems 
WHERE value_rating IS NOT NULL
GROUP BY price_category;
```

### Performance Analysis by Driver Type
```sql
SELECT 
    driver_type,
    COUNT(*) as total_products,
    AVG(price_msrp) as avg_price,
    COUNT(CASE WHEN rank IN ('S', 'A+', 'A') THEN 1 END) as top_tier_count
FROM headphones
WHERE driver_type IS NOT NULL
GROUP BY driver_type
ORDER BY avg_price DESC;
```

## Troubleshooting

### Common Issues:

1. **Connection Failed**: Check Redshift endpoint and security group settings
2. **Permission Denied**: Verify IAM role has S3 access permissions
3. **Table Not Found**: Ensure DDL scripts executed successfully

### Verification Commands:
```sql
-- Check if tables exist
SELECT tablename FROM pg_tables WHERE schemaname = 'public';

-- Verify data loaded
SELECT COUNT(*) FROM headphones;
SELECT COUNT(*) FROM iems;

-- Check recent loads
SELECT * FROM stl_load_commits WHERE filename LIKE '%headphones%' ORDER BY query DESC LIMIT 5;
```

## Cost Optimization

### For Development:
- Use `dc2.large` single-node cluster (default)
- Enable automated snapshots with 1-day retention
- Consider pausing cluster when not in use

### For Production:
- Scale to multi-node cluster as needed
- Implement data lifecycle policies
- Monitor query performance and optimize

## Security Notes

- Redshift cluster is in a private VPC
- Access controlled via security groups
- IAM roles used for S3 access (no hardcoded credentials)
- SSL connections enforced

## Next Steps

1. Set up automated data refresh schedule
2. Create data visualization dashboards
3. Implement alerting for data quality issues
4. Add more complex analytical queries