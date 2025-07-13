variable "aws_region" {
  description = "Region for the AWS services"
  type        = string
}

variable "bucket_prefix" {
  description = "Bucket prefix for the S3"
  type        = string
  default     = "rishabh-audiophile-e2e-pipeline"
}

variable "versioning" {
  type    = string
  default = "Enabled"
}

variable "aws_access_key" {
  description = "Access key for AWS"
  type        = string
  default     = ""
}

variable "aws_secret_key" {
  description = "Secret access key for AWS"
  type        = string
  default     = ""
}

# Redshift Configuration Variables
variable "redshift_cluster_identifier" {
  description = "Identifier for the Redshift cluster"
  type        = string
  default     = "audiophile-redshift-cluster"
}

variable "redshift_database_name" {
  description = "Name of the Redshift database"
  type        = string
  default     = "audiophile_db"
}

variable "redshift_master_username" {
  description = "Master username for Redshift cluster"
  type        = string
  default     = "admin"
}

variable "redshift_master_password" {
  description = "Master password for Redshift cluster"
  type        = string
  sensitive   = true
}

variable "redshift_node_type" {
  description = "Node type for Redshift cluster"
  type        = string
  default     = "dc2.large"
}

variable "redshift_cluster_type" {
  description = "Cluster type for Redshift (single-node or multi-node)"
  type        = string
  default     = "single-node"
}

variable "redshift_number_of_nodes" {
  description = "Number of nodes in the Redshift cluster"
  type        = number
  default     = 1
}
