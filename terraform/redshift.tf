resource "aws_redshift_cluster" "audiophile_cluster" {
  cluster_identifier = var.redshift_cluster_identifier
  database_name      = var.redshift_database_name
  master_username    = var.redshift_master_username
  master_password    = var.redshift_master_password
  
  node_type          = var.redshift_node_type
  cluster_type       = var.redshift_cluster_type
  number_of_nodes    = var.redshift_number_of_nodes
  
  # Security
  vpc_security_group_ids = [aws_security_group.redshift_sg.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet_group.name
  
  # Backup and maintenance
  skip_final_snapshot       = true
  final_snapshot_identifier = null
  automated_snapshot_retention_period = 1
  
  # Enhanced VPC routing for better S3 access
  enhanced_vpc_routing = true
  
  tags = {
    Name        = "audiophile-redshift-cluster"
    Environment = "dev"
    Project     = "audiophile-pipeline"
  }
}

# VPC for Redshift
resource "aws_vpc" "redshift_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = {
    Name = "audiophile-redshift-vpc"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "redshift_igw" {
  vpc_id = aws_vpc.redshift_vpc.id
  
  tags = {
    Name = "audiophile-redshift-igw"
  }
}

# Subnets for Redshift (need at least 2 in different AZs)
resource "aws_subnet" "redshift_subnet_1" {
  vpc_id            = aws_vpc.redshift_vpc.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
  
  tags = {
    Name = "audiophile-redshift-subnet-1"
  }
}

resource "aws_subnet" "redshift_subnet_2" {
  vpc_id            = aws_vpc.redshift_vpc.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]
  
  tags = {
    Name = "audiophile-redshift-subnet-2"
  }
}

# Data source for availability zones
data "aws_availability_zones" "available" {
  state = "available"
}

# Subnet group for Redshift
resource "aws_redshift_subnet_group" "redshift_subnet_group" {
  name       = "audiophile-redshift-subnet-group"
  subnet_ids = [aws_subnet.redshift_subnet_1.id, aws_subnet.redshift_subnet_2.id]
  
  tags = {
    Name = "audiophile-redshift-subnet-group"
  }
}

# Security group for Redshift
resource "aws_security_group" "redshift_sg" {
  name        = "audiophile-redshift-sg"
  description = "Security group for Audiophile Redshift cluster"
  vpc_id      = aws_vpc.redshift_vpc.id
  
  # Inbound rule for Redshift (port 5439)
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # In production, restrict this to specific IPs
  }
  
  # Outbound rules for S3 and internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name = "audiophile-redshift-sg"
  }
}

# IAM role for Redshift to access S3
resource "aws_iam_role" "redshift_s3_role" {
  name = "audiophile-redshift-s3-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
  
  tags = {
    Name = "audiophile-redshift-s3-role"
  }
}

# IAM policy for S3 access
resource "aws_iam_role_policy" "redshift_s3_policy" {
  name = "audiophile-redshift-s3-policy"
  role = aws_iam_role.redshift_s3_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.audiophile-bucket.arn,
          "${aws_s3_bucket.audiophile-bucket.arn}/*"
        ]
      }
    ]
  })
}

# Attach the role to the Redshift cluster
resource "aws_redshift_cluster_iam_roles" "redshift_cluster_iam_roles" {
  cluster_identifier = aws_redshift_cluster.audiophile_cluster.cluster_identifier
  iam_role_arns      = [aws_iam_role.redshift_s3_role.arn]
}

# Route table for public subnet access
resource "aws_route_table" "redshift_rt" {
  vpc_id = aws_vpc.redshift_vpc.id
  
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.redshift_igw.id
  }
  
  tags = {
    Name = "audiophile-redshift-rt"
  }
}

# Associate route table with subnets
resource "aws_route_table_association" "redshift_rta_1" {
  subnet_id      = aws_subnet.redshift_subnet_1.id
  route_table_id = aws_route_table.redshift_rt.id
}

resource "aws_route_table_association" "redshift_rta_2" {
  subnet_id      = aws_subnet.redshift_subnet_2.id
  route_table_id = aws_route_table.redshift_rt.id
}