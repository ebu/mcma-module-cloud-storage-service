import { AzureFunction, Context } from "@azure/functions";

import { AuthProvider, mcmaApiKeyAuth, ResourceManagerProvider } from "@mcma/client";
import { WorkerRequest } from "@mcma/worker";
import { AppInsightsLoggerProvider } from "@mcma/azure-logger";
import { AzureKeyVaultSecretsProvider } from "@mcma/azure-key-vault";
import { CosmosDbTableProvider, fillOptionsFromConfigVariables } from "@mcma/azure-cosmos-db";

import { buildWorker, WorkerContext } from "@local/worker";

const dbTableProvider = new CosmosDbTableProvider(fillOptionsFromConfigVariables());
const secretsProvider = new AzureKeyVaultSecretsProvider();
const authProvider = new AuthProvider().add(mcmaApiKeyAuth({ secretsProvider }));
const resourceManagerProvider = new ResourceManagerProvider(authProvider);
const loggerProvider = new AppInsightsLoggerProvider("cloud-storage-service-worker");

const worker = buildWorker(dbTableProvider, loggerProvider, resourceManagerProvider, secretsProvider);

export const handler: AzureFunction = async (context: Context) => {
    const queueMessage = context.bindings.queueMessage;
    const logger = loggerProvider.get(context.invocationId);

    try {
        logger.functionStart(context.invocationId);
        logger.debug(context);
        logger.debug(queueMessage);

        const workerContext: WorkerContext = {
            requestId: context.invocationId,
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
