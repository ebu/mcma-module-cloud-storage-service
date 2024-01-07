import { SecretsProvider } from "@mcma/secrets";
import { StorageClientFactory } from "./storage-client-factory";
import { WorkerInvoker } from "@mcma/worker-invoker";

export interface WorkerContext {
    requestId: string;
    functionTimeLimit: Date;
    storageClientFactory: StorageClientFactory;
    secretsProvider: SecretsProvider;
    workerInvoker: WorkerInvoker;
}
