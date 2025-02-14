#########################
# Provider registration
#########################

provider "azurerm" {
  tenant_id       = var.azure_tenant_id
  subscription_id = var.azure_subscription_id
  client_id       = var.AZURE_CLIENT_ID
  client_secret   = var.AZURE_CLIENT_SECRET

  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

provider "azapi" {
  tenant_id       = var.azure_tenant_id
  subscription_id = var.azure_subscription_id
  client_id       = var.AZURE_CLIENT_ID
  client_secret   = var.AZURE_CLIENT_SECRET
}

provider "mcma" {
  alias = "azure"

  service_registry_url = module.service_registry_azure.service_url

  mcma_api_key_auth {
    api_key = random_password.deployment_api_key.result
  }
}

######################
# Resource Group
######################

resource "azurerm_resource_group" "resource_group" {
  name     = "${var.prefix}-${var.azure_location}"
  location = var.azure_location
}

######################
# Resource Group east us
######################

resource "azurerm_resource_group" "resource_group_east_us" {
  name     = "${var.prefix}-eastus"
  location = "eastus"
}

######################
# App Storage Account
######################

resource "azurerm_storage_account" "storage_account" {
  name                     = format("%.24s", replace("${var.prefix}-${azurerm_resource_group.resource_group.location}", "/[^a-z0-9]+/", ""))
  resource_group_name      = azurerm_resource_group.resource_group.name
  location                 = azurerm_resource_group.resource_group.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

######################
# App Storage Account
######################

resource "azurerm_storage_account" "storage_account_east_us" {
  name                     = format("%.24s", replace("${var.prefix}-${azurerm_resource_group.resource_group_east_us.location}", "/[^a-z0-9]+/", ""))
  resource_group_name      = azurerm_resource_group.resource_group_east_us.name
  location                 = azurerm_resource_group.resource_group_east_us.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

######################
# Cosmos DB
######################

resource "azurerm_cosmosdb_account" "cosmosdb_account" {
  name                = var.prefix
  resource_group_name = azurerm_resource_group.resource_group.name
  location            = azurerm_resource_group.resource_group.location
  offer_type          = "Standard"

  consistency_policy {
    consistency_level = "Strong"
  }

  geo_location {
    failover_priority = 0
    location          = azurerm_resource_group.resource_group.location
  }

  capabilities {
    name = "EnableServerless"
  }
}

resource "azurerm_cosmosdb_sql_database" "cosmosdb_database" {
  name                = var.prefix
  resource_group_name = azurerm_resource_group.resource_group.name
  account_name        = azurerm_cosmosdb_account.cosmosdb_account.name
}

########################
# Application Insights
########################

resource "azurerm_log_analytics_workspace" "app_insights" {
  name                = var.prefix
  resource_group_name = azurerm_resource_group.resource_group.name
  location            = azurerm_resource_group.resource_group.location
}

resource "azurerm_application_insights" "app_insights" {
  name                = var.prefix
  resource_group_name = azurerm_resource_group.resource_group.name
  location            = azurerm_resource_group.resource_group.location
  workspace_id        = azurerm_log_analytics_workspace.app_insights.id
  application_type    = "web"
}

#########################
# Service Registry Module
#########################

module "service_registry_azure" {
  source = "github.com/ebu/mcma-module-service-registry//azure/module?ref=v1.0.0"

  prefix = "${var.prefix}-sr"

  resource_group    = azurerm_resource_group.resource_group
  storage_account   = azurerm_storage_account.storage_account
  app_insights      = azurerm_application_insights.app_insights
  cosmosdb_account  = azurerm_cosmosdb_account.cosmosdb_account
  cosmosdb_database = azurerm_cosmosdb_sql_database.cosmosdb_database

  use_flex_consumption_plan = true

  api_keys_read_only = [
    module.job_processor_azure.api_key,
    module.cloud_storage_service_azure.api_key,
  ]

  api_keys_read_write = [
    random_password.deployment_api_key.result
  ]

  key_vault_secret_expiration_date = "2200-01-01T00:00:00Z"
}

#########################
# Job Processor Module
#########################

module "job_processor_azure" {
  providers = {
    mcma = mcma.azure
  }

  source = "github.com/ebu/mcma-module-job-processor//azure/module?ref=v1.0.0"

  prefix = "${var.prefix}-jp"

  use_flex_consumption_plan = true

  resource_group    = azurerm_resource_group.resource_group
  storage_account   = azurerm_storage_account.storage_account
  app_insights      = azurerm_application_insights.app_insights
  cosmosdb_account  = azurerm_cosmosdb_account.cosmosdb_account
  cosmosdb_database = azurerm_cosmosdb_sql_database.cosmosdb_database

  service_registry = module.service_registry_azure

  api_keys_read_write = [
    random_password.deployment_api_key.result,
    module.cloud_storage_service_azure.api_key,
  ]

  key_vault_secret_expiration_date = "2200-01-01T00:00:00Z"
}

module "cloud_storage_service_azure" {
  providers = {
    mcma = mcma.azure
  }

  source = "../azure/module"

  prefix = "${var.prefix}-css"

  use_flex_consumption_plan = true

  resource_group    = azurerm_resource_group.resource_group
  storage_account   = azurerm_storage_account.storage_account
  app_insights      = azurerm_application_insights.app_insights
  cosmosdb_account  = azurerm_cosmosdb_account.cosmosdb_account
  cosmosdb_database = azurerm_cosmosdb_sql_database.cosmosdb_database

  service_registry = module.service_registry_azure

  api_keys_read_write = [
    random_password.deployment_api_key.result,
    module.job_processor_azure.api_key
  ]

  aws_s3_buckets = [
    {
      bucket     = aws_s3_bucket.private.id
      region     = aws_s3_bucket.private.region
      access_key = aws_iam_access_key.bucket_access.id
      secret_key = aws_iam_access_key.bucket_access.secret
    },
    {
      bucket     = aws_s3_bucket.private_ext.id
      region     = aws_s3_bucket.private_ext.region
      access_key = aws_iam_access_key.bucket_access.id
      secret_key = aws_iam_access_key.bucket_access.secret
    },
    {
      bucket     = aws_s3_bucket.target.id
      region     = aws_s3_bucket.target.region
      access_key = aws_iam_access_key.bucket_access.id
      secret_key = aws_iam_access_key.bucket_access.secret
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
      access_key = aws_iam_access_key.bucket_access.id
      secret_key = aws_iam_access_key.bucket_access.secret
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

  key_vault_secret_expiration_date = "2200-01-01T00:00:00Z"
}
