#################################
# Lambda worker
#################################

locals {
  lambda_name_worker = format("%.64s", replace("${var.prefix}-worker", "/[^a-zA-Z0-9_]+/", "-"))
}

resource "aws_iam_role" "worker" {
  name = format("%.64s", replace("${var.prefix}-${var.aws_region}-worker", "/[^a-zA-Z0-9_]+/", "-"))
  path = var.iam_role_path

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowLambdaAssumingRole"
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  permissions_boundary = var.iam_permissions_boundary

  tags = var.tags
}

resource "aws_iam_role_policy" "worker" {
  name = aws_iam_role.worker.name
  role = aws_iam_role.worker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid      = "DescribeCloudWatchLogs"
        Effect   = "Allow"
        Action   = "logs:DescribeLogGroups"
        Resource = "*"
      },
      {
        Sid    = "WriteToCloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ],
        Resource = concat([
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${var.log_group.name}:*",
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.lambda_name_worker}:*",
          ], var.enhanced_monitoring_enabled ? [
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda-insights:*",
        ] : [])
      },
      {
        Sid    = "ListAndDescribeDynamoDBTables"
        Effect = "Allow"
        Action = [
          "dynamodb:List*",
          "dynamodb:DescribeReservedCapacity*",
          "dynamodb:DescribeLimits",
          "dynamodb:DescribeTimeToLive",
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowTableOperations"
        Effect = "Allow"
        Action = [
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:DeleteItem",
          "dynamodb:DescribeTable",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:UpdateItem",
        ]
        Resource = [
          aws_dynamodb_table.service_table.arn,
          "${aws_dynamodb_table.service_table.arn}/index/*",
        ]
      },
      {
        Sid      = "AllowInvokingWorkerLambda"
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:${local.lambda_name_worker}"
      },
      {
        Sid    = "AllowReadingApiKey"
        Effect = "Allow"
        Action = "secretsmanager:GetSecretValue"
        Resource = [
          aws_secretsmanager_secret.api_key.arn,
          aws_secretsmanager_secret.storage_client_config.arn
        ]
      },
      ],
      length(var.execute_api_arns) > 0 ?
      [
        {
          Sid      = "AllowInvokingApiGateway"
          Effect   = "Allow"
          Action   = "execute-api:Invoke"
          Resource = var.execute_api_arns
        }
      ] : [],
      var.xray_tracing_enabled ?
      [
        {
          Sid    = "AllowLambdaWritingToXRay"
          Effect = "Allow"
          Action = [
            "xray:PutTraceSegments",
            "xray:PutTelemetryRecords",
            "xray:GetSamplingRules",
            "xray:GetSamplingTargets",
            "xray:GetSamplingStatisticSummaries",
          ]
          Resource = "*"
        }
      ] : [],
      var.dead_letter_config_target != null ?
      [
        {
          Effect   = "Allow"
          Action   = "sqs:SendMessage"
          Resource = var.dead_letter_config_target
        }
      ] : [],
      length(local.buckets_that_require_permissions) > 0 ?
      [
        {
          Sid      = "AllowS3Listing"
          Effect   = "Allow"
          Action   = ["s3:ListBucket"]
          Resource = local.buckets_that_require_permissions
        },
        {
          Sid      = "AllowS3Operations"
          Effect   = "Allow"
          Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
          Resource = [for bucket in local.buckets_that_require_permissions : "${bucket}/*"]
        }
      ] : [],
    )
  })
}

resource "aws_lambda_function" "worker" {
  depends_on = [
    aws_iam_role_policy.worker
  ]

  filename         = "${path.module}/lambdas/worker.zip"
  function_name    = local.lambda_name_worker
  role             = aws_iam_role.worker.arn
  handler          = "index.handler"
  source_code_hash = filebase64sha256("${path.module}/lambdas/worker.zip")
  runtime          = "nodejs18.x"
  timeout          = "900"
  memory_size      = var.worker_memory_size

  layers = var.enhanced_monitoring_enabled && contains(keys(local.lambda_insights_extensions), var.aws_region) ? [
    local.lambda_insights_extensions[var.aws_region]
  ] : []

  environment {
    variables = {
      MCMA_LOG_GROUP_NAME             = var.log_group.name
      MCMA_TABLE_NAME                 = aws_dynamodb_table.service_table.name
      MCMA_PUBLIC_URL                 = local.service_url
      MCMA_SERVICE_REGISTRY_URL       = var.service_registry.service_url
      MCMA_SERVICE_REGISTRY_AUTH_TYPE = var.service_registry.auth_type
      MCMA_WORKER_FUNCTION_ID         = local.lambda_name_worker
      MCMA_API_KEY_SECRET_ID          = aws_secretsmanager_secret.api_key.name
      JOB_PROFILE_PREFIX              = var.job_profile_prefix
      STORAGE_CLIENT_CONFIG_SECRET_ID = aws_secretsmanager_secret.storage_client_config.name
      STORAGE_CLIENT_CONFIG_HASH      = sha256(aws_secretsmanager_secret_version.storage_client_config.secret_string)
      MAX_CONCURRENCY                 = var.max_concurrency
      MULTIPART_SIZE                  = var.multipart_size
    }
  }

  dynamic "dead_letter_config" {
    for_each = var.dead_letter_config_target != null ? toset([1]) : toset([])

    content {
      target_arn = var.dead_letter_config_target
    }
  }

  tracing_config {
    mode = var.xray_tracing_enabled ? "Active" : "PassThrough"
  }

  tags = var.tags
}

locals {
  buckets_that_require_permissions = [
    for each in var.aws_s3_buckets : "arn:aws:s3:::${each.bucket}" if each.access_key == null || each.secret_key == null
  ]
}
