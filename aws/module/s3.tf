####################################
#  Bucket for temporary files
####################################
locals {
  bucket_name_temp = "${var.prefix}-temp-${var.aws_region}"
}

resource "aws_s3_bucket" "temp" {
  count = var.temp_bucket == null ? 1 : 0

  bucket = local.bucket_name_temp

  force_destroy = true

  tags = var.tags
}

resource "aws_s3_bucket_policy" "temp" {
  count  = length(aws_s3_bucket.temp)
  bucket = aws_s3_bucket.temp[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyHttpRequests",
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          "arn:aws:s3:::${local.bucket_name_temp}",
          "arn:aws:s3:::${local.bucket_name_temp}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      {
        Sid       = "DenyDeprecatedTlsRequests",
        Effect    = "Deny",
        Principal = "*",
        Action    = "s3:*",
        Resource = [
          "arn:aws:s3:::${local.bucket_name_temp}",
          "arn:aws:s3:::${local.bucket_name_temp}/*"
        ],
        Condition = {
          NumericLessThan = {
            "s3:TlsVersion" = "1.2"
          }
        }
      }
    ]
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "temp" {
  count  = length(aws_s3_bucket.temp)
  bucket = aws_s3_bucket.temp[0].id

  rule {
    id     = "Delete after 7 days"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
  }
}
