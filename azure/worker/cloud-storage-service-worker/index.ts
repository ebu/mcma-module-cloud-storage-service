import { AzureFunction, Context } from "@azure/functions";

import { AuthProvider, mcmaApiKeyAuth, ResourceManagerProvider } from "@mcma/client";
import { WorkerRequest } from "@mcma/worker";
import { AppInsightsLoggerProvider } from "@mcma/azure-logger";
import { AzureKeyVaultSecretsProvider } from "@mcma/azure-key-vault";
import { CosmosDbTableProvider, fillOptionsFromConfigVariables } from "@mcma/azure-cosmos-db";
import { QueueWorkerInvoker } from "@mcma/azure-queue-worker-invoker";

import { buildWorker, WorkerContext, StorageClientFactory } from "@local/worker";

const dbTableProvider = new CosmosDbTableProvider(fillOptionsFromConfigVariables());
const secretsProvider = new AzureKeyVaultSecretsProvider();
const authProvider = new AuthProvider().add(mcmaApiKeyAuth({ secretsProvider }));
const resourceManagerProvider = new ResourceManagerProvider(authProvider);
const loggerProvider = new AppInsightsLoggerProvider("cloud-storage-service-worker");
const workerInvoker = new QueueWorkerInvoker();

const storageClientFactory = new StorageClientFactory({
    secretsProvider,
});

const worker = buildWorker(dbTableProvider, loggerProvider, resourceManagerProvider, secretsProvider);

export const handler: AzureFunction = async (context: Context) => {
    const queueMessage = context.bindings.queueMessage;
    const logger = await loggerProvider.get(context.invocationId);

    try {
        logger.functionStart(context.invocationId);
        logger.debug(context);
        logger.debug(queueMessage);

        // assume 5 mins function timeout, so we stop in 3 mins from now
        let timeLimit = new Date(Date.now() + 180000);

        logger.info(process.env.AzureFunctionsJobHost__functionTimeout);
        const functionTimeout = process.env.AzureFunctionsJobHost__functionTimeout;
        if (functionTimeout) {
            const parts = functionTimeout.split(":");
            if (parts.length === 3) {
                const durationInSeconds = Number.parseInt(parts[0]) * 3600 + Number.parseInt(parts[1]) * 60 + Number.parseInt(parts[2]);
                timeLimit = new Date(Date.now() + (durationInSeconds - 120) * 1000);
            }
        }

        const workerContext: WorkerContext = {
            requestId: context.invocationId,
            secretsProvider,
            storageClientFactory,
            timeLimit,
            workerInvoker,
        };

        await worker.doWork(new WorkerRequest(queueMessage, logger), workerContext);
    } catch (error) {
        logger.error(error.message);
        logger.error(error);
    } finally {
        logger.functionEnd(context.invocationId);
        loggerProvider.flush();
    }
};
