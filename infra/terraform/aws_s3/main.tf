terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "kyc_lakehouse" {
  bucket = var.lakehouse_bucket_name

  tags = {
    Project = "real-time-financial-kyc-data-platform"
    Domain  = "financial-kyc"
  }
}

resource "aws_s3_bucket_versioning" "kyc_lakehouse" {
  bucket = aws_s3_bucket.kyc_lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}
