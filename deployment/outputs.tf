output "service_registry_aws" {
  value = {
    auth_type   = module.service_registry_aws.auth_type
    service_url = module.service_registry_aws.service_url
  }
}

output "job_processor_aws" {
  sensitive = true
  value = {
    auth_type   = module.job_processor_aws.auth_type
    service_url = module.job_processor_aws.service_url
    api_key     = module.job_processor_aws.api_key
  }
}

output "cloud_storage_service_aws" {
  sensitive = true
  value = {
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
  value = {
    auth_type   = module.job_processor_azure.auth_type
    service_url = module.job_processor_azure.service_url
    api_key     = module.job_processor_azure.api_key
  }
}

output "cloud_storage_service_azure" {
  sensitive = true
  value = {
    auth_type   = module.cloud_storage_service_azure.auth_type
    service_url = module.cloud_storage_service_azure.service_url
    api_key     = module.cloud_storage_service_azure.api_key
  }
}

output "deployment_api_key" {
  sensitive = true
  value     = random_password.deployment_api_key.result
}

output "deployment_prefix" {
  value = var.prefix
}

output "aws_region" {
  value = var.aws_region
}

output "azure_location" {
  value = var.azure_location
}

output "storage_locations" {
  sensitive = true
  value = {
    awsS3Buckets = [
      {
        bucket    = aws_s3_bucket.private.id
        region    = aws_s3_bucket.private.region
        accessKey = aws_iam_access_key.bucket_access.id
        secretKey = aws_iam_access_key.bucket_access.secret
      },
      {
        bucket    = aws_s3_bucket.private_ext.id
        region    = aws_s3_bucket.private_ext.region
        accessKey = aws_iam_access_key.bucket_access.id
        secretKey = aws_iam_access_key.bucket_access.secret
      },
      {
        bucket    = aws_s3_bucket.target.id
        region    = aws_s3_bucket.target.region
        accessKey = aws_iam_access_key.bucket_access.id
        secretKey = aws_iam_access_key.bucket_access.secret
      },
      {
        bucket    = var.s3_like_bucket_name
        region    = "us-east-1"
        accessKey = var.s3_like_bucket_access_key
        secretKey = var.s3_like_bucket_secret_key
        endpoint  = var.s3_like_bucket_endpoint
      },
      {
        bucket    = aws_s3_bucket.private_eu_west_1.id
        region    = aws_s3_bucket.private_eu_west_1.region
        accessKey = aws_iam_access_key.bucket_access.id
        secretKey = aws_iam_access_key.bucket_access.secret
      },
    ]

    azureStorageAccounts = [
      {
        account          = azurerm_storage_account.storage_account.name
        connectionString = azurerm_storage_account.storage_account.primary_connection_string
      },
      {
        account          = azurerm_storage_account.storage_account_east_us.name
        connectionString = azurerm_storage_account.storage_account_east_us.primary_connection_string
      },
    ]
  }
}
