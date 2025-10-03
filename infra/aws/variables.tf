variable "project_prefix" {
  description = "Prefix for bucket/role names (e.g., aqs-qa, aqs-dev, aqs-prod)"
  type        = string
  default     = "aqs-qa"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "snowflake_aws_account_id" {
  description = "Snowflake AWS account ID for your Snowflake region"
  type        = string
}

variable "external_id" {
  description = "External ID from `DESC INTEGRATION` after you create the Snowflake storage integration"
  type        = string
}
