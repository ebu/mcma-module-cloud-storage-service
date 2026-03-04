import { captureAWSv3Client as xrayCaptureAwsV3Client, SegmentLike } from "aws-xray-sdk-core";
import { Context } from "aws-lambda";
import { CloudWatchLogsClient } from "@aws-sdk/client-cloudwatch-logs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand, S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { LambdaWorkerInvoker } from "@mcma/aws-lambda-worker-invoker";
import { ConfigVariables, ConsoleLoggerProvider, LoggerProvider } from "@mcma/core";

import { AuthProvider, mcmaApiKeyAuth, ResourceManagerProvider } from "@mcma/client";
import { WorkerRequest, WorkerRequestProperties } from "@mcma/worker";
import { DynamoDbTableProvider } from "@mcma/aws-dynamodb";
import { AwsCloudWatchLoggerProvider, getLogGroupName } from "@mcma/aws-logger";
import { awsV4Auth } from "@mcma/aws-client";
import { AwsSecretsManagerSecretsProvider } from "@mcma/aws-secrets-manager";

import { StorageClientFactory, FileCopierState, saveTrieToS3, loadTrieFromS3 } from "@local/storage";
import { buildWorker, WorkerContext } from "@local/worker";

function captureAWSv3Client<T extends { middlewareStack: { remove: any, use: any }, config: any }>(client: T, manualSegment?: SegmentLike): T {
    if (process.env.RUN_LOCAL === "true") {
        return client;
    }
    return xrayCaptureAwsV3Client(client);
}

function buildLoggerProvider(): LoggerProvider {
    if (process.env.RUN_LOCAL === "true") {
        return new ConsoleLoggerProvider("cloud-storage-service-worker");
    }
    return new AwsCloudWatchLoggerProvider("cloud-storage-service-worker", getLogGroupName(), captureAWSv3Client(new CloudWatchLogsClient({})));
}

const dynamoDBClient = captureAWSv3Client(new DynamoDBClient({}));
const lambdaClient = captureAWSv3Client(new LambdaClient({}));
const secretsManagerClient = captureAWSv3Client(new SecretsManagerClient({}));

const secretsProvider = new AwsSecretsManagerSecretsProvider({ client: secretsManagerClient });
const authProvider = new AuthProvider().add(awsV4Auth()).add(mcmaApiKeyAuth({ secretsProvider }));
const dbTableProvider = new DynamoDbTableProvider({}, dynamoDBClient);
const loggerProvider = buildLoggerProvider();
const resourceManagerProvider = new ResourceManagerProvider(authProvider);
const workerInvoker = new LambdaWorkerInvoker(lambdaClient);

const buildS3Client: (config: S3ClientConfig) => S3Client = config => !config.endpoint ? captureAWSv3Client(new S3Client(config)) : new S3Client(config);

const configVariables = new ConfigVariables();
const s3ClientForState = buildS3Client({});

const storageClientFactory = new StorageClientFactory({
    secretsProvider,
    buildS3Client,
});

const worker = buildWorker(dbTableProvider, loggerProvider, resourceManagerProvider, secretsProvider);

function computeBucketAndKey(jobAssignmentId: string) {
    const bucket = configVariables.get("TEMP_BUCKET");
    const jobAssignmentGuid = jobAssignmentId.substring(jobAssignmentId.lastIndexOf("/") + 1);
    const stateKey = `${configVariables.get("TEMP_BUCKET_PREFIX")}${jobAssignmentGuid}.json`;
    const trieKey = `${configVariables.get("TEMP_BUCKET_PREFIX")}${jobAssignmentGuid}.trie.gz`;
    return { bucket, stateKey, trieKey };
}

async function loadFileCopierState(jobAssignmentId: string): Promise<FileCopierState> {
    const { bucket, stateKey, trieKey } = computeBucketAndKey(jobAssignmentId);

    const output = await s3ClientForState.send(new GetObjectCommand({
        Bucket: bucket,
        Key: stateKey,
    }));

    const content = await output.Body.transformToString();
    const state: any = JSON.parse(content);
    const trie = await loadTrieFromS3(s3ClientForState, bucket, trieKey);

    return {
        filesTotal: state.filesTotal,
        filesCopied: state.filesCopied,
        bytesTotal: state.bytesTotal,
        bytesCopied: state.bytesCopied,
        workItems: state.workItems,
        trie
    }
}

async function saveFileCopierState(jobAssignmentId: string, state: FileCopierState): Promise<void> {
    const { bucket, stateKey, trieKey } = computeBucketAndKey(jobAssignmentId);

    await s3ClientForState.send(new PutObjectCommand({
        Bucket: bucket,
        Key: stateKey,
        Body: JSON.stringify({
            filesTotal: state.filesTotal,
            filesCopied: state.filesCopied,
            bytesTotal: state.bytesTotal,
            bytesCopied: state.bytesCopied,
            workItems: state.workItems,
        }),
    }));

    await saveTrieToS3(state.trie, s3ClientForState, bucket, trieKey);
}

async function deleteFileCopierState(jobAssignmentId: string): Promise<void> {
    const { bucket, stateKey, trieKey } = computeBucketAndKey(jobAssignmentId);

    await s3ClientForState.send(new DeleteObjectCommand({
        Bucket: bucket,
        Key: stateKey,
    }));

    await s3ClientForState.send(new DeleteObjectCommand({
        Bucket: bucket,
        Key: trieKey,
    }));
}

export async function handler(event: WorkerRequestProperties, context: Context) {
    const logger = await loggerProvider.get(context.awsRequestId, event.tracker);

    try {
        logger.functionStart(context.awsRequestId);
        logger.debug(event);
        logger.debug(context);

        const workerContext: WorkerContext = {
            requestId: context.awsRequestId,
            functionTimeLimit: new Date(Date.now() + context.getRemainingTimeInMillis()),
            secretsProvider,
            storageClientFactory,
            workerInvoker,
            loadFileCopierState,
            saveFileCopierState,
            deleteFileCopierState,
        };

        await worker.doWork(new WorkerRequest(event, logger), workerContext);
    } catch (error) {
        logger.error("Error occurred when handling operation '" + event.operationName + "'");
        logger.error(error);
    } finally {
        logger.functionEnd(context.awsRequestId);
        if (process.env.RUN_LOCAL !== "true") {
            await (loggerProvider as AwsCloudWatchLoggerProvider).flush();
        }
    }
}

