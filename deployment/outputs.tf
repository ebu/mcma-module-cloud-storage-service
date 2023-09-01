output "service_registry_aws" {
  value = {
    auth_type   = module.service_registry_aws.auth_type
    service_url = module.service_registry_aws.service_url
  }
}

output "job_processor_aws" {
  sensitive = true
  value     = {
    auth_type   = module.job_processor_aws.auth_type
    service_url = module.job_processor_aws.service_url
    api_key     = module.job_processor_aws.api_key
  }
}

output "cloud_storage_service_aws" {
  sensitive = true
  value     = {
    auth_type   = module.cloud_storage_service_aws.auth_type
    service_url = module.cloud_storage_service_aws.service_url
    api_key     = module.cloud_storage_service_aws.api_key
  }
}

output "service_registry_azure" {
  value = {
    auth_type   = module.service_registry_azure.auth_type
    service_url = module.service_registry_azure.service_url
  }
}

output "job_processor_azure" {
  sensitive = true
  value     = {
    auth_type   = module.job_processor_azure.auth_type
    service_url = module.job_processor_azure.service_url
    api_key     = module.job_processor_azure.api_key
  }
}

output "cloud_storage_service_azure" {
  sensitive = true
  value     = {
    auth_type   = module.cloud_storage_service_azure.auth_type
    service_url = module.cloud_storage_service_azure.service_url
    api_key     = module.cloud_storage_service_azure.api_key
  }
}

output "deployment_api_key" {
  sensitive = true
  value     = random_password.deployment_api_key.result
}

output "aws_private_source_bucket" {
  value = aws_s3_bucket.private.id
}

output "aws_public_source_bucket" {
  value = aws_s3_bucket.public.id
}

output "aws_private_ext_source_bucket" {
  value = aws_s3_bucket.private_ext.id
}

output "aws_target_bucket" {
  value = aws_s3_bucket.target.id
}

output "azure_storage_connection_string" {
  sensitive = true
  value     = azurerm_storage_account.app_storage_account.primary_connection_string
}

output "azure_source_container" {
  value = azurerm_storage_container.source.name
}

output "azure_target_container" {
  value = azurerm_storage_container.target.name
}
