#########################
# Environment Variables
#########################

variable "name" {
  type        = string
  description = "Optional variable to set a custom name for this service in the service registry"
  default     = "Cloud Storage Service"
}

variable "prefix" {
  type        = string
  description = "Prefix for all managed resources in this module"
}

variable "log_group" {
  type = object({
    id   = string
    arn  = string
    name = string
  })
  description = "Log group used by MCMA Event tracking"
}

variable "dead_letter_config_target" {
  type        = string
  description = "Configuring dead letter target for worker lambda"
  default     = null
}

variable "tags" {
  type        = map(string)
  description = "Tags applied to created resources"
  default     = {}
}

#########################
# AWS Variables
#########################

variable "aws_region" {
  type        = string
  description = "AWS Region to which this module is deployed"
}

variable "iam_role_path" {
  type        = string
  description = "Path for creation of access role"
  default     = "/"
}

variable "iam_permissions_boundary" {
  type        = string
  description = "IAM permissions boundary"
  default     = null
}

#########################
# Dependencies
#########################

variable "service_registry" {
  type = object({
    auth_type   = string,
    service_url = string,
  })
}

variable "execute_api_arns" {
  type        = list(string)
  description = "Optional ist of api gateway execution arns that will allow you to control which API's the lambdas are allowed to invoke"
  default     = []
}

#########################
# Configuration
#########################

variable "api_gateway_metrics_enabled" {
  type        = bool
  description = "Enable API Gateway metrics"
  default     = false
}

variable "xray_tracing_enabled" {
  type        = bool
  description = "Enable X-Ray tracing"
  default     = false
}

variable "enhanced_monitoring_enabled" {
  type        = bool
  description = "Enable CloudWatch Lambda Insights"
  default     = false
}

#########################
# MCMA Api Key Authentication
#########################

variable "api_keys_read_only" {
  type    = list(string)
  default = []
}

variable "api_keys_read_write" {
  type    = list(string)
  default = []
}

#########################
# Selecting API Authentication
#########################

variable "api_security_auth_type" {
  type    = string
  default = "McmaApiKey"

  validation {
    condition     = var.api_security_auth_type == null || can(regex("^(AWS4|McmaApiKey)$", var.api_security_auth_type))
    error_message = "ERROR: Valid auth types are \"AWS4\" and \"McmaApiKey\"!"
  }
}

########################
# Storage access
########################

variable "aws_s3_buckets" {
  type = list(object({
    bucket     = string
    region     = string
    access_key = optional(string)
    secret_key = optional(string)
    endpoint   = optional(string)
  }))
}

variable "azure_storage_accounts" {
  type = list(object({
    account           = string
    connection_string = string
  }))
}
