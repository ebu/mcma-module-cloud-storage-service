resource "aws_secretsmanager_secret" "api_key_security_config" {
  count = var.api_security_auth_type == "McmaApiKey" ? 1 : 0

  name_prefix             = "${var.prefix}-api-key-security-config"
  recovery_window_in_days = 0
}

locals {
  api_keys_read_only = {
    for api_key in var.api_keys_read_only :
    api_key => {}
  }
  api_keys_read_write = merge({
    for api_key in var.api_keys_read_write :
    api_key => {
      "^/job-assignments(?:/.+)?$" = ["ANY"]
    }
  })
}

resource "aws_secretsmanager_secret_version" "api_key_security_config" {
  count = var.api_security_auth_type == "McmaApiKey" ? 1 : 0

  secret_id     = aws_secretsmanager_secret.api_key_security_config[0].id
  secret_string = jsonencode(merge({
    "no-auth"    = {}
    "valid-auth" = {
      "^/job-assignments(?:/.+)?$" = ["GET"]
    }
  },
    local.api_keys_read_only,
    local.api_keys_read_write
  ))
}

resource "aws_secretsmanager_secret" "api_key" {
  name_prefix             = "${var.prefix}-api-key"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "api_key" {
  secret_id     = aws_secretsmanager_secret.api_key.id
  secret_string = random_password.api_key.result
}

resource "random_password" "api_key" {
  length  = 32
  special = false
}

locals {
  aws_config = merge({
    for each in var.aws_s3_buckets :
    each.bucket => {
      region    = each.region
      accessKey = each.access_key
      secretKey = each.secret_key
      endpoint  = each.endpoint
    }
  })
  azure_config = merge({
    for each in var.azure_storage_accounts :
    each.account => {
      connectionString = each.connection_string
    }
  })
}

resource "aws_secretsmanager_secret" "storage_client_config" {
  name_prefix             = "${var.prefix}-storage-client-config"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "storage_client_config" {
  secret_id     = aws_secretsmanager_secret.storage_client_config.id
  secret_string = jsonencode({
    aws   = local.aws_config
    azure = local.azure_config
  })
}
