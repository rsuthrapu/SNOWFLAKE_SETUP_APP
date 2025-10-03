terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  buckets  = ["${var.project_prefix}-raw", "${var.project_prefix}-curated", "${var.project_prefix}-analytics"]
  role_name = "${var.project_prefix}-snowflake-s3-role"
}

# Buckets
resource "aws_s3_bucket" "lz" {
  for_each = toset(local.buckets)
  bucket   = each.value
  tags = {
    Project = var.project_prefix
    Purpose = "Snowflake Landing Zone"
  }
}

resource "aws_s3_bucket_public_access_block" "lz" {
  for_each                = aws_s3_bucket.lz
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lz" {
  for_each = aws_s3_bucket.lz
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "lz" {
  for_each = aws_s3_bucket.lz
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Policy allowing Snowflake to use the buckets
data "aws_iam_policy_document" "snowflake_bucket_access" {
  statement {
    sid     = "ListBucket"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.lz : b.arn]
  }

  statement {
    sid     = "ObjectRW"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
    ]
    resources = [for b in aws_s3_bucket.lz : "${b.arn}/*"]
  }
}

resource "aws_iam_policy" "snowflake_s3_policy" {
  name   = "${var.project_prefix}-snowflake-s3-policy"
  policy = data.aws_iam_policy_document.snowflake_bucket_access.json
}

# Trust policy: require Snowflake AWS account + ExternalId
data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    sid    = "TrustSnowflake"
    effect = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.snowflake_aws_account_id}:root"]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_role" {
  name               = local.role_name
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_policy.arn
}

output "bucket_names" {
  value = [for b in aws_s3_bucket.lz : b.bucket]
}

output "role_arn" {
  value = aws_iam_role.snowflake_role.arn
}
