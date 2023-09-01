import { StorageClientFactory } from "./storage-client-factory";

export interface WorkerContext {
    requestId: string;
    storageClientFactory: StorageClientFactory;
}
