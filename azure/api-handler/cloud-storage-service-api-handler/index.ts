import { Context, HttpRequest, AzureFunction } from "@azure/functions";

import { McmaApiKeySecurityMiddleware, DefaultJobRouteCollection } from "@mcma/api";
import { CosmosDbTableProvider, fillOptionsFromConfigVariables } from "@mcma/azure-cosmos-db";
import { AppInsightsLoggerProvider } from "@mcma/azure-logger";
import { AzureFunctionApiController } from "@mcma/azure-functions-api";
import { AzureKeyVaultSecretsProvider } from "@mcma/azure-key-vault";
import { QueueWorkerInvoker } from "@mcma/azure-queue-worker-invoker";

const loggerProvider = new AppInsightsLoggerProvider("cloud-storage-service-api-handler");
const dbTableProvider = new CosmosDbTableProvider(fillOptionsFromConfigVariables());
const secretsProvider = new AzureKeyVaultSecretsProvider();
const workerInvoker = new QueueWorkerInvoker();

const securityMiddleware = new McmaApiKeySecurityMiddleware({ secretsProvider });

const routes = new DefaultJobRouteCollection(dbTableProvider, workerInvoker);

const restController =
    new AzureFunctionApiController(
        {
            routes,
            loggerProvider,
            middleware: [securityMiddleware],
        });

export const handler: AzureFunction = async (context: Context, request: HttpRequest) => {
    const logger = loggerProvider.get(context.invocationId);
    try {
        logger.functionStart(context.invocationId);
        logger.debug(context);
        logger.debug(request);

        return await restController.handleRequest(request);
    } catch (error) {
        logger.error(error);
        throw error;
    } finally {
        logger.functionEnd(context.invocationId);
        loggerProvider.flush();
    }
};
