output "aws_region" {
  description = "Region set for AWS"
  value       = var.aws_region
}

output "bucket_name" {
  description = "S3 bucket name."
  value       = aws_s3_bucket.audiophile-bucket.id
}

# Redshift Outputs
output "redshift_cluster_endpoint" {
  description = "Redshift cluster endpoint"
  value       = aws_redshift_cluster.audiophile_cluster.endpoint
}

output "redshift_cluster_id" {
  description = "Redshift cluster identifier"
  value       = aws_redshift_cluster.audiophile_cluster.cluster_identifier
}

output "redshift_database_name" {
  description = "Redshift database name"
  value       = aws_redshift_cluster.audiophile_cluster.database_name
}

output "redshift_port" {
  description = "Redshift cluster port"
  value       = aws_redshift_cluster.audiophile_cluster.port
}

output "redshift_iam_role_arn" {
  description = "IAM role ARN for Redshift S3 access"
  value       = aws_iam_role.redshift_s3_role.arn
}