import { captureAWSv3Client } from "aws-xray-sdk-core";
import { Context } from "aws-lambda";
import { CloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";

import { AuthProvider, mcmaApiKeyAuth, ResourceManagerProvider } from "@mcma/client";
import { WorkerRequest, WorkerRequestProperties } from "@mcma/worker";
import { DynamoDbTableProvider } from "@mcma/aws-dynamodb";
import { AwsCloudWatchLoggerProvider, getLogGroupName } from "@mcma/aws-logger";
import { awsV4Auth } from "@mcma/aws-client";
import { AwsSecretsManagerSecretsProvider } from "@mcma/aws-secrets-manager";

import { buildWorker, WorkerContext, StorageClientFactory } from "@local/worker";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { LambdaWorkerInvoker } from "@mcma/aws-lambda-worker-invoker";

const cloudWatchLogsClient = captureAWSv3Client(new CloudWatchLogsClient({}));
const dynamoDBClient = captureAWSv3Client(new DynamoDBClient({}));
const lambdaClient = captureAWSv3Client(new LambdaClient({}));
const secretsManagerClient = captureAWSv3Client(new SecretsManagerClient({}));

const secretsProvider = new AwsSecretsManagerSecretsProvider({ client: secretsManagerClient });
const authProvider = new AuthProvider().add(awsV4Auth()).add(mcmaApiKeyAuth({ secretsProvider }));
const dbTableProvider = new DynamoDbTableProvider({}, dynamoDBClient);
const loggerProvider = new AwsCloudWatchLoggerProvider("cloud-storage-service-worker", getLogGroupName(), cloudWatchLogsClient);
const resourceManagerProvider = new ResourceManagerProvider(authProvider);
const workerInvoker = new LambdaWorkerInvoker(lambdaClient);

const buildS3Client: (config: S3ClientConfig) => S3Client = config => !config.endpoint ? captureAWSv3Client(new S3Client(config)) : new S3Client(config);

const storageClientFactory = new StorageClientFactory({
    secretsProvider,
    buildS3Client,
});

const worker = buildWorker(dbTableProvider, loggerProvider, resourceManagerProvider, secretsProvider);

export async function handler(event: WorkerRequestProperties, context: Context) {
    const logger = await loggerProvider.get(context.awsRequestId, event.tracker);

    try {
        logger.functionStart(context.awsRequestId);
        logger.debug(event);
        logger.debug(context);

        const workerContext: WorkerContext = {
            requestId: context.awsRequestId,
            secretsProvider,
            storageClientFactory,
            functionTimeLimit: new Date(Date.now() + context.getRemainingTimeInMillis()),
            workerInvoker,
        };

        await worker.doWork(new WorkerRequest(event, logger), workerContext);
    } catch (error) {
        logger.error("Error occurred when handling operation '" + event.operationName + "'");
        logger.error(error);
    } finally {
        logger.functionEnd(context.awsRequestId);
        await loggerProvider.flush();
    }
}
