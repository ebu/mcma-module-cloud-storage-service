#########################
# Provider registration
#########################

provider "aws" {
  profile = var.aws_profile
  region  = var.aws_region
}

provider "aws" {
  profile = var.aws_profile
  region  = "eu-west-1"
  alias   = "eu_west_1"
}

provider "mcma" {
  alias = "aws"

  service_registry_url       = module.service_registry_aws.service_url
  service_registry_auth_type = module.service_registry_aws.auth_type

  aws4_auth {
    profile = var.aws_profile
    region  = var.aws_region
  }

  mcma_api_key_auth {
    api_key = random_password.deployment_api_key.result
  }
}

############################################
# Cloud watch log group for central logging
############################################

resource "aws_cloudwatch_log_group" "main" {
  name = "/mcma/${var.prefix}"
}

#########################
# Service Registry Module
#########################

module "service_registry_aws" {
  source = "github.com/ebu/mcma-module-service-registry//aws/module?ref=v1.2.0"

  prefix = "${var.prefix}-service-registry"

  aws_region = var.aws_region

  log_group                   = aws_cloudwatch_log_group.main
  api_gateway_metrics_enabled = true
  xray_tracing_enabled        = true
  enhanced_monitoring_enabled = true

  api_keys_read_only = [
    module.job_processor_aws.api_key,
    module.cloud_storage_service_aws.api_key
  ]
  api_keys_read_write = [
    random_password.deployment_api_key.result
  ]
}

##########################
## Job Processor Module
##########################

module "job_processor_aws" {
  providers = {
    mcma = mcma.aws
  }

  source = "github.com/ebu/mcma-module-job-processor//aws/module?ref=v1.2.0"

  prefix = "${var.prefix}-job-processor"

  dashboard_name = var.prefix

  aws_region = var.aws_region

  service_registry = module.service_registry_aws

  log_group                   = aws_cloudwatch_log_group.main
  api_gateway_metrics_enabled = true
  xray_tracing_enabled        = true

  api_keys_read_write = [
    random_password.deployment_api_key.result,
    module.cloud_storage_service_aws.api_key
  ]
}

module "cloud_storage_service_aws" {
  providers = {
    mcma = mcma.aws
  }

  source = "../aws/module"

  prefix = "${var.prefix}-cloud-storage-service"

  aws_region = var.aws_region

  service_registry = module.service_registry_aws

  log_group                   = aws_cloudwatch_log_group.main
  api_gateway_metrics_enabled = true
  xray_tracing_enabled        = true

  api_keys_read_write = [
    random_password.deployment_api_key.result,
    module.job_processor_aws.api_key
  ]

  aws_s3_buckets = [
    {
      bucket = aws_s3_bucket.archive.id
      region = aws_s3_bucket.archive.region
    },
    {
      bucket = aws_s3_bucket.private.id
      region = aws_s3_bucket.private.region
    },
    {
      bucket     = aws_s3_bucket.private_ext.id
      region     = aws_s3_bucket.private_ext.region
      access_key = nonsensitive(aws_iam_access_key.bucket_access.id)
      secret_key = nonsensitive(aws_iam_access_key.bucket_access.secret)
    },
    {
      bucket = aws_s3_bucket.target.id
      region = aws_s3_bucket.target.region
    },
    {
      bucket     = var.s3_like_bucket_name
      region     = "us-east-1"
      access_key = var.s3_like_bucket_access_key
      secret_key = var.s3_like_bucket_secret_key
      endpoint   = var.s3_like_bucket_endpoint
    },
    {
      bucket     = aws_s3_bucket.private_eu_west_1.id
      region     = aws_s3_bucket.private_eu_west_1.region
      access_key = nonsensitive(aws_iam_access_key.bucket_access.id)
      secret_key = nonsensitive(aws_iam_access_key.bucket_access.secret)
    },
  ]

  azure_storage_accounts = [
    {
      account           = azurerm_storage_account.storage_account.name
      connection_string = azurerm_storage_account.storage_account.primary_connection_string
    },
    {
      account           = azurerm_storage_account.storage_account_east_us.name
      connection_string = azurerm_storage_account.storage_account_east_us.primary_connection_string
    },
  ]
}
