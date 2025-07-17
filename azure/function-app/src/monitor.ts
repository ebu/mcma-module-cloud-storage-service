import { v4 as uuidv4 } from "uuid";

import { InvocationContext, Timer } from "@azure/functions";
import { McmaTracker } from "@mcma/core";
import { CosmosDbTableProvider, fillOptionsFromConfigVariables } from "@mcma/azure-cosmos-db";
import { AppInsightsLoggerProvider } from "@mcma/azure-logger";
import { QueueWorkerInvoker } from "@mcma/azure-queue-worker-invoker";

import { StorageClientFactory } from "@local/storage";
import { Monitor } from "@local/monitor";
import { AzureKeyVaultSecretsProvider } from "@mcma/azure-key-vault";

const loggerProvider = new AppInsightsLoggerProvider("cloud-storage-service-monitor");
const dbTableProvider = new CosmosDbTableProvider(fillOptionsFromConfigVariables());
const secretsProvider = new AzureKeyVaultSecretsProvider();
const workerInvoker = new QueueWorkerInvoker();

const storageClientFactory = new StorageClientFactory({
    secretsProvider,
});

export async function monitor(timer: Timer, context: InvocationContext) {
    const tracker = new McmaTracker({
        id: uuidv4(),
        label: "Monitor - " + new Date().toUTCString()
    });

    const logger = await loggerProvider.get(context.invocationId, tracker);
    try {
        logger.functionStart(context.invocationId);
        logger.debug(context);
        logger.debug(timer);

        const monitor = new Monitor({
            dbTableProvider,
            storageClientFactory,
            workerInvoker,
        });

        await monitor.run(logger);
    } catch (error) {
        logger.error(error);
        throw error;
    } finally {
        logger.functionEnd(context.invocationId);
        loggerProvider.flush();
    }
}
