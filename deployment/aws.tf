#########################
# Provider registration
#########################

provider "aws" {
  profile = var.aws_profile
  region  = var.aws_region
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
  source = "https://ch-ebu-mcma-module-repository.s3.eu-central-1.amazonaws.com/ebu/service-registry/aws/0.16.1-beta6/module.zip"

  prefix = "${var.prefix}-service-registry"

  stage_name = var.environment_type

  aws_region  = var.aws_region
  aws_profile = var.aws_profile

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

#########################
# Job Processor Module
#########################

module "job_processor_aws" {
  providers = {
    mcma = mcma.aws
  }

  source = "https://ch-ebu-mcma-module-repository.s3.eu-central-1.amazonaws.com/ebu/job-processor/aws/0.16.1-beta3/module.zip"

  prefix = "${var.prefix}-job-processor"

  stage_name     = var.environment_type
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

  source = "../aws/build/staging"

  prefix = "${var.prefix}-cloud-storage-service"

  stage_name = var.environment_type
  aws_region = var.aws_region

  service_registry = module.service_registry_aws

  log_group                   = aws_cloudwatch_log_group.main
  api_gateway_metrics_enabled = true
  xray_tracing_enabled        = true

  api_keys_read_write = [
    random_password.deployment_api_key.result
  ]
}
