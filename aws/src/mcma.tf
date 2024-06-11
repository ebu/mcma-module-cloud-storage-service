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
    mcma_job_profile.copy_folder.id,
  ]
}

resource "mcma_job_profile" "copy_file" {
  name = "CloudCopyFile"

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
  name = "CloudCopyFolder"

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
