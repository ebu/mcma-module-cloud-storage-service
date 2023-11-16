import { Locator, StorageJob } from "@mcma/core";
import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { doCopyFile, SourceFile, TargetFile, WorkerContext } from "../index";

export async function copyFile(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;

    logger.info(jobInput);

    const sourceLocator = jobInput.sourceFile as Locator;
    const targetLocator = jobInput.targetFile as Locator;

    const sourceFile: SourceFile = {
        locator: sourceLocator,
        alternateUrl: jobInput.alternateUrl,
        alternateAuthType: jobInput.alternateAuthType,
    };

    const targetFile: TargetFile = {
        locator: targetLocator
    };

    const getS3Client = async (bucket: string) => ctx.storageClientFactory.getS3Client(bucket);
    const getContainerClient = async (account: string, container: string) => ctx.storageClientFactory.getContainerClient(account, container);

    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);

    await jobAssignmentHelper.complete();
}
