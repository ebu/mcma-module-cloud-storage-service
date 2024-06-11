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

variable "tags" {
  type        = map(string)
  description = "Tags applied to created resources"
  default     = {}
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
  default = null
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

variable "max_concurrency" {
  type        = number
  description = "Set number of max concurrent transfers"
  default     = 8
}

variable "multipart_size" {
  type        = number
  description = "Set multipart size for files bigger than this value"
  default     = 67108864   // 64MiB
}

variable "worker_function_timeout" {
  type        = string
  description = "Set the timeout for the worker function. Valid values depend on chosen app service plan"
  default     = "00:10:00"
}

variable "function_elastic_instance_minimum" {
  type        = number
  description = "Set the minimum instance number for azure functions when using premium plan"
  default     = null
}
