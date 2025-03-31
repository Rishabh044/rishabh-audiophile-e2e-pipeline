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
