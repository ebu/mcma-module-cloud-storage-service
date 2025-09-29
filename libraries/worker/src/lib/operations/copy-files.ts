import { JobStatus, Locator, ProblemDetail, StorageJob, Utils } from "@mcma/core";
import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { getWorkerFunctionId } from "@mcma/worker-invoker";

import { FileCopier, SourceFile, DestinationFile, logError, saveFileCopierState } from "@local/storage";
import { WorkerContext } from "../worker-context";
import { scanSourceFolderForCopy } from "./utils";

const { MAX_CONCURRENCY, MULTIPART_SIZE } = process.env;

export async function copyFiles(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;
    logger.info(jobInput);

    if (jobAssignmentHelper.job.status === JobStatus.Completed || jobAssignmentHelper.job.status === JobStatus.Failed || jobAssignmentHelper.job.status === JobStatus.Canceled) {
        return;
    }

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

    const runUntilDate = new Date(ctx.functionTimeLimit.getTime() - 120000);
    const bailOutDate = new Date(ctx.functionTimeLimit.getTime() - 10000);
    const abortTimeout = ctx.functionTimeLimit.getTime() - Date.now() - 30000;

    const fileCopier = new FileCopier({
        maxConcurrency: Number.parseInt(MAX_CONCURRENCY),
        multipartSize: Number.parseInt(MULTIPART_SIZE),
        logger,
        getS3Client,
        getContainerClient,
        progressUpdate,
        axiosConfig: {
            signal: AbortSignal.timeout(abortTimeout)
        }
    });

    const transfers: { source: Locator, sourceEgressUrl?: string, destination: Locator }[] = jobInput.transfers;

    if (!Array.isArray(transfers) || transfers.length < 1) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/invalid-input",
            title: "CopyFiles job profile requires property transfers as input with at least 1 element",
            detail: !Array.isArray(transfers) ? "Property 'transfers' is not an array" : "Property 'transfers' doesn't contain any element"
        }));
        return;
    }

    for (const transfer of transfers) {
        const sourceLocator = transfer.source as Locator;
        const targetLocator = transfer.destination as Locator;

        const sourceFile: SourceFile = {
            locator: sourceLocator,
            egressUrl: transfer.sourceEgressUrl,
        };

        const destinationFile: DestinationFile = {
            locator: targetLocator
        };

        let transfers2: { sourceFile: SourceFile, destinationFile: DestinationFile }[];
        try {
            transfers2 = await scanSourceFolderForCopy(sourceFile, destinationFile, ctx);
        } catch (error) {
            logger.warn(`Failed to scan source folder for ${sourceFile.locator.url} due to following error. Assuming provided locators are files.`);
            logger.warn(error);
            transfers2 = [{ sourceFile, destinationFile }];
        }
        logger.info(transfers2);

        for (const transfer2 of transfers2) {
            fileCopier.addFile(transfer2.sourceFile, transfer2.destinationFile);
        }
    }

    await fileCopier.runUntil(runUntilDate, bailOutDate);

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
