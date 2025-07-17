import { SecretsProvider } from "@mcma/secrets";
import { StorageClientFactory } from "@local/storage";
import { WorkerInvoker } from "@mcma/worker-invoker";

export interface WorkerContext {
    requestId: string;
    functionTimeLimit: Date;
    storageClientFactory: StorageClientFactory;
    secretsProvider: SecretsProvider;
    workerInvoker: WorkerInvoker;
}
