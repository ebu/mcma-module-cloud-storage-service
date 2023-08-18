import { APIGatewayProxyEvent, Context } from "aws-lambda";
import * as AWSXRay from "aws-xray-sdk-core";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";

import { DefaultJobRouteCollection, McmaApiKeySecurityMiddleware, McmaApiMiddleware } from "@mcma/api";
import { DynamoDbTableProvider } from "@mcma/aws-dynamodb";
import { LambdaWorkerInvoker } from "@mcma/aws-lambda-worker-invoker";
import { ApiGatewayApiController } from "@mcma/aws-api-gateway";
import { ConsoleLoggerProvider } from "@mcma/core";
import { AwsSecretsManagerSecretsProvider } from "@mcma/aws-secrets-manager";

const secretsManagerClient = AWSXRay.captureAWSv3Client(new SecretsManagerClient({}));

const dynamoDBClient = AWSXRay.captureAWSv3Client(new DynamoDBClient({}));
const lambdaClient = AWSXRay.captureAWSv3Client(new LambdaClient({}));

const secretsProvider = new AwsSecretsManagerSecretsProvider({ client: secretsManagerClient });
const dbTableProvider = new DynamoDbTableProvider({}, dynamoDBClient);
const loggerProvider = new ConsoleLoggerProvider("cloud-storage-service-api-handler");
const workerInvoker = new LambdaWorkerInvoker(lambdaClient);

const routes = new DefaultJobRouteCollection(dbTableProvider, workerInvoker);

const middleware: McmaApiMiddleware[] = [];

if (process.env.MCMA_API_KEY_SECURITY_CONFIG_SECRET_ID) {
    const securityMiddleware = new McmaApiKeySecurityMiddleware({ secretsProvider });
    middleware.push(securityMiddleware);
}

const restController = new ApiGatewayApiController({
    routes,
    loggerProvider
});

export async function handler(event: APIGatewayProxyEvent, context: Context) {
    const logger = loggerProvider.get(context.awsRequestId);
    try {
        logger.functionStart(context.awsRequestId);
        logger.debug(event);
        logger.debug(context);

        return await restController.handleRequest(event, context);
    } catch (error) {
        logger.error(error);
        throw error;
    } finally {
        logger.functionEnd(context.awsRequestId);
    }
}
