########################################
# Public S3 Bucket
########################################
resource "aws_s3_bucket" "public" {
  bucket = "${var.prefix}-public-${var.aws_region}"

  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "public" {
  bucket = aws_s3_bucket.public.id

  rule {
    id     = "Delete after 1 day"
    status = "Enabled"
    expiration {
      days = 1
    }
  }
}

resource "aws_s3_bucket_public_access_block" "public" {
  bucket = aws_s3_bucket.public.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "public" {
  depends_on = [aws_s3_bucket_public_access_block.public]

  bucket = aws_s3_bucket.public.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = [
          "s3:GetObject"
        ]
        Resource : [
          "arn:aws:s3:::${aws_s3_bucket.public.id}/*"
        ]
      }
    ]
  })
}

########################################
# Private Source S3 Bucket
########################################
resource "aws_s3_bucket" "private" {
  bucket = "${var.prefix}-private-${var.aws_region}"

  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "private" {
  bucket = aws_s3_bucket.private.id

  rule {
    id     = "Delete after 1 day"
    status = "Enabled"
    expiration {
      days = 1
    }
  }
}

########################################
# Private External Source S3 Bucket
########################################
resource "aws_s3_bucket" "private_ext" {
  bucket = "${var.prefix}-private-ext-${var.aws_region}"

  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "private_ext" {
  bucket = aws_s3_bucket.private_ext.id

  rule {
    id     = "Delete after 1 day"
    status = "Enabled"
    expiration {
      days = 1
    }
  }
}

resource "aws_iam_user" "private_ext" {
  name = "${var.prefix}-private-ext-access"
}

resource "aws_iam_access_key" "bucket_access" {
  user = aws_iam_user.private_ext.id
}

resource "aws_iam_user_policy" "bucket_access" {
  user   = aws_iam_user.private_ext.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.private_ext.id}/*",
          "arn:aws:s3:::${aws_s3_bucket.private.id}/*",
          "arn:aws:s3:::${aws_s3_bucket.target.id}/*"
        ]
      }
    ]
  })
}

########################################
# Target S3 Bucket
########################################
resource "aws_s3_bucket" "target" {
  bucket = "${var.prefix}-target-${var.aws_region}"

  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "target" {
  bucket = aws_s3_bucket.target.id

  rule {
    id     = "Delete after 1 day"
    status = "Enabled"
    expiration {
      days = 1
    }
  }
}


########################################
# Source Blob Storage Container
########################################
resource "azurerm_storage_container" "source" {
  name = "${var.prefix}-source-${azurerm_resource_group.resource_group.location}"

  storage_account_name = azurerm_storage_account.app_storage_account.name
}

########################################
# Target Blob Storage Container
########################################
resource "azurerm_storage_container" "target" {
  name = "${var.prefix}-target-${azurerm_resource_group.resource_group.location}"

  storage_account_name = azurerm_storage_account.app_storage_account.name
}
