import { SecretsProvider } from "@mcma/secrets";
import { StorageClientFactory, FileCopierState } from "@local/storage";
import { WorkerInvoker } from "@mcma/worker-invoker";

export interface WorkerContext {
    requestId: string;
    functionTimeLimit: Date;
    loadFileCopierState: (jobAssignmentDatabaseId: string) => Promise<FileCopierState>;
    saveFileCopierState: (jobAssignmentDatabaseId: string, state: FileCopierState) => Promise<void>;
    deleteFileCopierState: (jobAssignmentDatabaseId: string) => Promise<void>;
    storageClientFactory: StorageClientFactory;
    secretsProvider: SecretsProvider;
    workerInvoker: WorkerInvoker;
}
