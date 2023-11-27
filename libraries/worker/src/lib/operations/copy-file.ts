import { Locator, ProblemDetail, StorageJob } from "@mcma/core";
import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { getWorkerFunctionId } from "@mcma/worker-invoker";

import { SourceFile, TargetFile, WorkerContext, FileCopier } from "../index";

const { MAX_CONCURRENCY, MULTIPART_SIZE } = process.env;

export async function copyFile(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;
    logger.info(jobInput);

    const getS3Client = async (bucket: string, region?: string) => ctx.storageClientFactory.getS3Client(bucket, region);
    const getContainerClient = async (account: string, container: string) => ctx.storageClientFactory.getContainerClient(account, container);

    const progressUpdate = async (filesTotal: number, filesCopied: number, bytesTotal: number, bytesCopied: number) => {
        if (bytesTotal > 0) {
            const percentage = Math.round((bytesCopied / bytesTotal * 100 + Number.EPSILON) * 10) / 10;
            logger.info(`${percentage}%`);
            await jobAssignmentHelper.updateJobAssignment(jobAssigment => jobAssigment.progress = percentage, true);
        }
    };

    const fileCopier = new FileCopier({
        maxConcurrency: Number.parseInt(MAX_CONCURRENCY),
        multipartSize: Number.parseInt(MULTIPART_SIZE),
        logger,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });

    const sourceLocator = jobInput.sourceFile as Locator;
    const targetLocator = jobInput.targetFile as Locator;

    const sourceFile: SourceFile = {
        locator: sourceLocator,
        egressUrl: jobInput.sourceEgressUrl,
        egressAuthType: jobInput.sourceEgressAuthType,
    };

    const targetFile: TargetFile = {
        locator: targetLocator
    };

    fileCopier.addFile(sourceFile, targetFile);

    await fileCopier.runUntil(ctx.timeLimit);

    const error = fileCopier.getError();
    if (error) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/copy-failure",
            title: "Copy failure",
            detail: error.message,
        }));
        return;
    }

    const workItems = fileCopier.getWorkItems();
    if (workItems.length > 0) {
        logger.info(`${workItems.length} work items remaining. Invoking worker again`);
        const jobAssignmentDatabaseId = jobAssignmentHelper.jobAssignmentDatabaseId;
        const workItemsDatabaseId = jobAssignmentDatabaseId + "/work-items";
        await jobAssignmentHelper.dbTable.put(workItemsDatabaseId, { workItems });
        await ctx.workerInvoker.invoke(getWorkerFunctionId(), {
            operationName: "ContinueCopy",
            input: {
                jobAssignmentDatabaseId,
                workItemsDatabaseId,
            },
            tracker: jobAssignmentHelper.workerRequest.tracker
        });
        return;
    }

    await jobAssignmentHelper.complete();
}
