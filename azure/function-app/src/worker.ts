import { InvocationContext } from "@azure/functions";

import { AuthProvider, mcmaApiKeyAuth, ResourceManagerProvider } from "@mcma/client";
import { WorkerRequest } from "@mcma/worker";
import { AppInsightsLoggerProvider } from "@mcma/azure-logger";
import { AzureKeyVaultSecretsProvider } from "@mcma/azure-key-vault";
import { CosmosDbTableProvider, fillOptionsFromConfigVariables } from "@mcma/azure-cosmos-db";
import { QueueWorkerInvoker } from "@mcma/azure-queue-worker-invoker";
import { ConfigVariables } from "@mcma/core";
import { ContainerClient } from "@azure/storage-blob";
import { DefaultAzureCredential } from "@azure/identity";

import { FileCopierState, StorageClientFactory } from "@local/storage";
import { buildWorker, WorkerContext } from "@local/worker";

const loggerProvider = new AppInsightsLoggerProvider("cloud-storage-service-worker");
const dbTableProvider = new CosmosDbTableProvider(fillOptionsFromConfigVariables());
const secretsProvider = new AzureKeyVaultSecretsProvider();
const authProvider = new AuthProvider().add(mcmaApiKeyAuth({ secretsProvider }));
const resourceManagerProvider = new ResourceManagerProvider(authProvider);
const workerInvoker = new QueueWorkerInvoker();

const configVariables = new ConfigVariables();

const storageClientFactory = new StorageClientFactory({
    secretsProvider,
});

let containerClient: ContainerClient;

const worker = buildWorker(dbTableProvider, loggerProvider, resourceManagerProvider, secretsProvider);

function getBlobClient(jobAssignmentId: string) {
    if (!containerClient) {
        const accountName = configVariables.get("TEMP_STORAGE_ACCOUNT_NAME");
        const containerName = configVariables.get("TEMP_CONTAINER_NAME");

        const credential = new DefaultAzureCredential();

        containerClient = new ContainerClient(
            `https://${accountName}.blob.core.windows.net/${containerName}`,
            credential
        );
    }

    const prefix = configVariables.get("TEMP_CONTAINER_PREFIX");
    return containerClient.getBlockBlobClient(`${prefix}${jobAssignmentId}.json`);
}

async function loadFileCopierState(jobAssignmentId: string): Promise<FileCopierState> {
    const blobClient = getBlobClient(jobAssignmentId);
    const buffer = await blobClient.downloadToBuffer();
    const text = buffer.toString("utf-8");
    return JSON.parse(text);
}

async function saveFileCopierState(jobAssignmentId: string, state: FileCopierState): Promise<void> {
    const blobClient = getBlobClient(jobAssignmentId);
    const content = JSON.stringify(state);
    await blobClient.upload(content, Buffer.byteLength(content), {
        blobHTTPHeaders: {
            blobContentType: "application/json"
        },
    });
}

async function deleteFileCopierState(jobAssignmentId: string): Promise<void> {
    const blobClient = getBlobClient(jobAssignmentId);
    await blobClient.deleteIfExists();
}

export async function workerQueueHandler(queueItem: unknown, context: InvocationContext) {
    const queueMessage = queueItem as any;
    const logger = await loggerProvider.get(context.invocationId, queueMessage.tracker);

    try {
        logger.functionStart(context.invocationId);
        logger.debug(context);
        logger.debug(queueMessage);

        // assume 5 mins function timeout
        let functionTimeLimit = new Date(Date.now() + 300000);

        logger.info("AzureFunctionsJobHost__functionTimeout = " + process.env.AzureFunctionsJobHost__functionTimeout);
        const functionTimeout = process.env.AzureFunctionsJobHost__functionTimeout;
        if (functionTimeout) {
            const parts = functionTimeout.split(":");
            if (parts.length === 3) {
                const durationInSeconds = Number.parseInt(parts[0]) * 3600 + Number.parseInt(parts[1]) * 60 + Number.parseInt(parts[2]);
                functionTimeLimit = new Date(Date.now() + durationInSeconds * 1000);
            }
        }

        const workerContext: WorkerContext = {
            requestId: context.invocationId,
            secretsProvider,
            storageClientFactory,
            functionTimeLimit,
            workerInvoker,
            loadFileCopierState,
            saveFileCopierState,
            deleteFileCopierState,
        };

        await worker.doWork(new WorkerRequest(queueMessage, logger), workerContext);
    } catch (error) {
        logger.error(error.message);
        logger.error(error);
    } finally {
        logger.functionEnd(context.invocationId);
        loggerProvider.flush();
    }
}
