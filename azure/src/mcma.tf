resource "mcma_service" "service" {
  depends_on = [
    azurerm_cosmosdb_sql_database.service,
    azurerm_cosmosdb_sql_container.service,
    azurerm_key_vault.service,
    azurerm_key_vault_access_policy.deployment,
    azurerm_key_vault_access_policy.api_handler,
    azurerm_key_vault_access_policy.worker,
    azurerm_key_vault_secret.api_key,
    azurerm_key_vault_secret.api_key_security_config,
    azurerm_windows_function_app.api_handler,
    azurerm_windows_function_app.worker,
    azurerm_storage_queue.worker,
    azurerm_role_assignment.queue_contributor,
    azurerm_role_assignment.queue_sender,
  ]

  name     = var.name
  job_type = "StorageJob"
  auth_type = local.auth_type

  resource {
    resource_type = "JobAssignment"
    http_endpoint = "${local.service_url}/job-assignments"
  }

  job_profile_ids = [
    mcma_job_profile.copy_file.id,
    mcma_job_profile.copy_folder.id,
  ]
}

resource "mcma_job_profile" "copy_file" {
  name = "${var.job_profile_prefix}CopyFile"

  input_parameter {
    name = "sourceFile"
    type = "Locator"
  }

  input_parameter {
    name = "sourceEgressUrl"
    type = "string"
    optional = true
  }

  input_parameter {
    name = "sourceEgressAuthType"
    type = "string"
    optional = true
  }

  input_parameter {
    name = "targetFile"
    type = "Locator"
  }
}
resource "mcma_job_profile" "copy_folder" {
  name = "${var.job_profile_prefix}CopyFolder"

  input_parameter {
    name = "sourceFolder"
    type = "Locator"
  }

  input_parameter {
    name = "sourceEgressUrl"
    type = "string"
    optional = true
  }

  input_parameter {
    name = "sourceEgressAuthType"
    type = "string"
    optional = true
  }

  input_parameter {
    name = "targetFolder"
    type = "Locator"
  }
}
