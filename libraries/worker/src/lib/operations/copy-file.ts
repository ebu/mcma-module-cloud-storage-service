import { Locator, ProblemDetail, StorageJob, Utils } from "@mcma/core";
import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { getWorkerFunctionId } from "@mcma/worker-invoker";

import { SourceFile, TargetFile, WorkerContext, FileCopier } from "../index";
import { logError, saveFileCopierState } from "./utils";

const { MAX_CONCURRENCY, MULTIPART_SIZE } = process.env;

export async function copyFile(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;
    logger.info(jobInput);

    const jobAssignmentDatabaseId = jobAssignmentHelper.jobAssignmentDatabaseId;

    const getS3Client = async (bucket: string, region?: string) => ctx.storageClientFactory.getS3Client(bucket, region);
    const getContainerClient = async (account: string, container: string) => ctx.storageClientFactory.getContainerClient(account, container);

    const progressUpdate = async (filesTotal: number, filesCopied: number, bytesTotal: number, bytesCopied: number) => {
        if (bytesTotal > 0) {
            const progress = Math.round((bytesCopied / bytesTotal * 100 + Number.EPSILON) * 10) / 10;
            logger.info(`${progress}%`);

            if (typeof jobAssignmentHelper.jobAssignment.progress !== "number" || Math.abs(jobAssignmentHelper.jobAssignment.progress - progress) > 0.5) {
                await jobAssignmentHelper.updateJobAssignment(jobAssigment => jobAssigment.progress = progress, true);
            }
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
        logger.error("Failing job as copy resulted in a failure");
        logError(logger, error);
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/copy-failure",
            title: "Copy failure",
            detail: error.message,
        }));
        return;
    }

    const state = fileCopier.getState();
    if (state.workItems.length > 0) {
        logger.info(`${state.workItems.length} work items remaining. Storing FileCopierState`);
        await saveFileCopierState(state, jobAssignmentDatabaseId, jobAssignmentHelper.dbTable);

        logger.info(`Invoking worker again`);
        await ctx.workerInvoker.invoke(getWorkerFunctionId(), {
            operationName: "ContinueCopy",
            input: {
                jobAssignmentDatabaseId,
            },
            tracker: jobAssignmentHelper.workerRequest.tracker
        });
        return;
    }

    await Utils.sleep(1000);
    logger.info("Copy was a success, marking job as Completed");
    await jobAssignmentHelper.complete();
}
