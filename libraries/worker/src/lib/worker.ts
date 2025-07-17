import { LoggerProvider, StorageJob } from "@mcma/core";
import { ResourceManagerProvider } from "@mcma/client";
import { ProcessJobAssignmentOperation, ProviderCollection, Worker } from "@mcma/worker";
import { SecretsProvider } from "@mcma/secrets";
import { DocumentDatabaseTableProvider } from "@mcma/data";
import { copyFile, copyFolder, continueCopy, copyFiles, restoreFile, restoreFiles, restoreFolder, completeRestore } from "./operations";

export function buildWorker(dbTableProvider: DocumentDatabaseTableProvider, loggerProvider: LoggerProvider, resourceManagerProvider: ResourceManagerProvider, secretsProvider: SecretsProvider) {
    const providerCollection = new ProviderCollection({
        dbTableProvider,
        loggerProvider,
        resourceManagerProvider,
        secretsProvider,
    });

    const processJobAssignmentOperation = new ProcessJobAssignmentOperation(StorageJob)
        .addProfile(`${process.env.JOB_PROFILE_PREFIX ?? ""}CopyFile`, copyFile)
        .addProfile(`${process.env.JOB_PROFILE_PREFIX ?? ""}CopyFiles`, copyFiles)
        .addProfile(`${process.env.JOB_PROFILE_PREFIX ?? ""}CopyFolder`, copyFolder)
        .addProfile(`${process.env.JOB_PROFILE_PREFIX ?? ""}RestoreFile`, restoreFile)
        .addProfile(`${process.env.JOB_PROFILE_PREFIX ?? ""}RestoreFiles`, restoreFiles)
        .addProfile(`${process.env.JOB_PROFILE_PREFIX ?? ""}RestoreFolder`, restoreFolder);

    return new Worker(providerCollection)
        .addOperation(processJobAssignmentOperation)
        .addOperation("ContinueCopy", continueCopy)
        .addOperation("CompleteRestore", completeRestore)
}
