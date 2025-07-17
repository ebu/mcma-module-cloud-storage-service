import { CloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import { S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { Context, ScheduledEvent } from "aws-lambda";
import { captureAWSv3Client } from "aws-xray-sdk";

import { AwsCloudWatchLoggerProvider, getLogGroupName } from "@mcma/aws-logger";
import { AwsSecretsManagerSecretsProvider } from "@mcma/aws-secrets-manager";
import { DynamoDbTableProvider } from "@mcma/aws-dynamodb";
import { LambdaWorkerInvoker } from "@mcma/aws-lambda-worker-invoker";

import { StorageClientFactory } from "@local/storage";
import { Monitor } from "@local/monitor";
import { getTableName } from "@mcma/data";

const cloudWatchLogsClient = captureAWSv3Client(new CloudWatchLogsClient({}));
const dynamoDBClient = captureAWSv3Client(new DynamoDBClient({}));
const lambdaClient = captureAWSv3Client(new LambdaClient({}));
const secretsManagerClient = captureAWSv3Client(new SecretsManagerClient({}));

const dbTableProvider = new DynamoDbTableProvider({}, dynamoDBClient);
const secretsProvider = new AwsSecretsManagerSecretsProvider({ client: secretsManagerClient });
const loggerProvider = new AwsCloudWatchLoggerProvider("cloud-storage-service-monitor", getLogGroupName(), cloudWatchLogsClient);
const workerInvoker = new LambdaWorkerInvoker(lambdaClient);

const buildS3Client: (config: S3ClientConfig) => S3Client = config => !config.endpoint ? captureAWSv3Client(new S3Client(config)) : new S3Client(config);

const storageClientFactory = new StorageClientFactory({
    secretsProvider,
    buildS3Client,
});

export async function handler(event: ScheduledEvent, context: Context) {
    const logger = await loggerProvider.get(context.awsRequestId);

    const table = await dbTableProvider.get(getTableName());
    const mutex = table.createMutex({
        name: "cloud-storage-service-monitor",
        holder: context.awsRequestId,
        logger,
        lockTimeout: 15 * 60 * 1000, // 15 mins lock
    });

    const lock = await mutex.tryLock();
    if (!lock) {
        return;
    }
    try {
        logger.functionStart(context.awsRequestId);
        logger.debug(event);
        logger.debug(context);

        const monitor = new Monitor({
            dbTableProvider,
            storageClientFactory,
            workerInvoker,
        });
        await monitor.run(logger);

    } catch (error) {
        logger.error(error);
    } finally {
        await mutex.unlock();
        logger.functionEnd(context.awsRequestId);
        await loggerProvider.flush();
    }
}
