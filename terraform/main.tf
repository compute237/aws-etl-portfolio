terraform {
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

variable "aws_region"   { default = "us-east-1" }
variable "project_name" { default = "etl-pipeline" }
variable "environment"  { default = "production" }

locals {
  prefix = "${var.project_name}-${var.environment}"
}

# ──────────────────────────────────────────
# S3 Buckets
# ──────────────────────────────────────────

resource "aws_s3_bucket" "raw" {
  bucket = "${local.prefix}-raw-data"
  tags   = { env = var.environment, purpose = "raw-ingestion" }
}

resource "aws_s3_bucket" "processed" {
  bucket = "${local.prefix}-processed-data"
  tags   = { env = var.environment, purpose = "etl-output" }
}

resource "aws_s3_bucket" "scripts" {
  bucket = "${local.prefix}-glue-scripts"
  tags   = { env = var.environment, purpose = "glue-scripts" }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue/etl_job.py"
  source = "${path.module}/../glue/etl_job.py"
  etag   = filemd5("${path.module}/../glue/etl_job.py")
}

# ──────────────────────────────────────────
# IAM Role for Glue
# ──────────────────────────────────────────

resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.prefix}-glue-s3-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.processed.arn}/*"]
      }
    ]
  })
}

# ──────────────────────────────────────────
# Glue Job
# ──────────────────────────────────────────

resource "aws_glue_job" "etl" {
  name         = "${local.prefix}-glue-job"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue/etl_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--source_bucket"                    = aws_s3_bucket.raw.bucket
    "--dest_bucket"                      = aws_s3_bucket.processed.bucket
  }

  execution_property {
    max_concurrent_runs = 1
  }

  number_of_workers = 2
  worker_type       = "G.1X"

  tags = { env = var.environment }
}

# ──────────────────────────────────────────
# Glue Trigger — nightly schedule (2am UTC)
# ──────────────────────────────────────────

resource "aws_glue_trigger" "nightly" {
  name     = "${local.prefix}-nightly-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 2 * * ? *)"

  actions {
    job_name = aws_glue_job.etl.name
  }

  tags = { env = var.environment }
}

# ──────────────────────────────────────────
# CloudWatch — alert on job failure
# ──────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "glue_failure" {
  alarm_name          = "${local.prefix}-glue-job-failed"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "ETL job has failed tasks"

  dimensions = {
    JobName = aws_glue_job.etl.name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}

resource "aws_sns_topic" "alerts" {
  name = "${local.prefix}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = "your-email@example.com"
}

# ──────────────────────────────────────────
# Data + outputs
# ──────────────────────────────────────────

data "aws_caller_identity" "current" {}

output "raw_bucket"       { value = aws_s3_bucket.raw.bucket }
output "processed_bucket" { value = aws_s3_bucket.processed.bucket }
output "glue_job_name"    { value = aws_glue_job.etl.name }
