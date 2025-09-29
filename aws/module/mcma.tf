resource "mcma_service" "service" {
  depends_on = [
    aws_apigatewayv2_api.service_api,
    aws_apigatewayv2_integration.service_api,
    aws_apigatewayv2_route.service_api_default,
    aws_apigatewayv2_route.service_api_options,
    aws_apigatewayv2_stage.service_api,
    aws_dynamodb_table.service_table,
    aws_iam_role.api_handler,
    aws_iam_role_policy.api_handler,
    aws_lambda_function.api_handler,
    aws_lambda_permission.service_api_default,
    aws_lambda_permission.service_api_options,
  ]

  name      = var.name
  job_type  = "StorageJob"
  auth_type = var.api_security_auth_type

  resource {
    resource_type = "JobAssignment"
    http_endpoint = "${local.service_url}/job-assignments"
  }

  job_profile_ids = [
    mcma_job_profile.copy_file.id,
    mcma_job_profile.copy_files.id,
    mcma_job_profile.copy_folder.id,
    mcma_job_profile.restore_file.id,
    mcma_job_profile.restore_files.id,
    mcma_job_profile.restore_folder.id,
  ]
}

resource "mcma_job_profile" "copy_file" {
  name = "${var.job_profile_prefix}CopyFile"

  input_parameter {
    name = "sourceFile"
    type = "Locator"
  }

  input_parameter {
    name     = "sourceEgressUrl"
    type     = "string"
    optional = true
  }

  input_parameter {
    name = "destinationFile"
    type = "Locator"
  }
}

resource "mcma_job_profile" "copy_files" {
  name = "${var.job_profile_prefix}CopyFiles"

  input_parameter {
    name = "transfers"
    type = "{ source: Locator, sourceEgressUrl?: string, destination: Locator }[]"
  }
}

resource "mcma_job_profile" "copy_folder" {
  name = "${var.job_profile_prefix}CopyFolder"

  input_parameter {
    name = "sourceFolder"
    type = "Locator"
  }

  input_parameter {
    name     = "sourceEgressUrl"
    type     = "string"
    optional = true
  }

  input_parameter {
    name = "destinationFolder"
    type = "Locator"
  }
}

resource "mcma_job_profile" "restore_file" {
  name = "${var.job_profile_prefix}RestoreFile"

  input_parameter {
    name = "file"
    type = "Locator"
  }

  input_parameter {
    name     = "priority"
    type     = "string"
    optional = true
  }

  input_parameter {
    name     = "durationInDays"
    type     = "number"
    optional = true
  }
}

resource "mcma_job_profile" "restore_files" {
  name = "${var.job_profile_prefix}RestoreFiles"

  input_parameter {
    name = "locators"
    type = "Locator[]"
  }

  input_parameter {
    name     = "priority"
    type     = "string"
    optional = true
  }

  input_parameter {
    name     = "durationInDays"
    type     = "number"
    optional = true
  }
}

resource "mcma_job_profile" "restore_folder" {
  name = "${var.job_profile_prefix}RestoreFolder"

  input_parameter {
    name = "folder"
    type = "Locator"
  }

  input_parameter {
    name     = "priority"
    type     = "string"
    optional = true
  }

  input_parameter {
    name     = "durationInDays"
    type     = "number"
    optional = true
  }
}
