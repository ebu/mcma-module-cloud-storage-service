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

variable "resource_group" {
  type = object({
    name     = string
    location = string
  })
}

###########################
# Azure accounts and plans
###########################

variable "app_storage_account" {
  type = object({
    name               = string
    primary_access_key = string
  })
}

variable "app_service_plan" {
  type = object({
    id   = string
    name = string
  })
}

variable "cosmosdb_account" {
  type = object({
    name        = string
    endpoint    = string
    primary_key = string
  })
}

variable "cosmosdb_database" {
  type = object({
    name = string
  })
  default = null
}

variable "app_insights" {
  type = object({
    name                = string
    connection_string   = string
    instrumentation_key = string
  })
}

#######################
# API authentication
#######################

variable "api_keys_read_only" {
  type    = list(string)
  default = []
}

variable "api_keys_read_write" {
  type    = list(string)
  default = []
}

variable "key_vault_secret_expiration_date" {
  type    = string
  default = null
}

#########################
# Custom Job Types
#########################

variable "custom_job_types" {
  type        = list(string)
  description = "Optionally add custom job types"
  default     = []
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

####################################
# Configuration for copy to AWS S3
####################################

variable "aws_s3_copy_max_concurrency" {
  type        = number
  description = "Set number of max concurrency while doing copy between S3 buckets"
  default     = 16
}

variable "aws_url_copy_max_concurrency" {
  type        = number
  description = "Set number of max concurrency while doing copy from url to S3 buckets"
  default     = 8
}

variable "worker_function_timeout" {
  type        = string
  description = "Set the timeout for the worker function. Valid values depend on chosen app service plan"
  default     = "00:10:00"
}
