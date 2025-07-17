import { JobStatus, ProblemDetail, Utils } from "@mcma/core";
import { getTableName } from "@mcma/data";
import { ProcessJobAssignmentHelper, ProviderCollection, WorkerRequest } from "@mcma/worker";
import { getWorkerFunctionId } from "@mcma/worker-invoker";

import { FileCopier, deleteFileCopierState, loadFileCopierState, logError, saveFileCopierState } from "@local/storage";

import { WorkerContext } from "../worker-context";

const { MAX_CONCURRENCY, MULTIPART_SIZE } = process.env;

export async function continueCopy(providers: ProviderCollection, workerRequest: WorkerRequest, ctx?: WorkerContext) {
    const jobAssignmentHelper = new ProcessJobAssignmentHelper(
        await providers.dbTableProvider.get(getTableName()),
        providers.resourceManagerProvider.get(),
        workerRequest
    );

    const jobAssignmentDatabaseId = jobAssignmentHelper.jobAssignmentDatabaseId;
    const logger = jobAssignmentHelper.logger;

    try {
        await jobAssignmentHelper.initialize();

        if (jobAssignmentHelper.job.status === JobStatus.Completed || jobAssignmentHelper.job.status === JobStatus.Failed || jobAssignmentHelper.job.status === JobStatus.Canceled) {
            return;
        }

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

        {
            const state = await loadFileCopierState(jobAssignmentDatabaseId, jobAssignmentHelper.dbTable);

            if (!state.workItems.length) {
                logger.error("Failed to retrieve remaining work items from database. Failing Job");
                await jobAssignmentHelper.fail(new ProblemDetail({
                    type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/generic-failure",
                    title: "Generic failure",
                    detail: "Failed to retrieve remaining work items from database",
                }));
                return;
            }

            logger.info(`Loaded ${state.workItems.length} work items`);
            fileCopier.setState(state);
        }

        let continueRunning = true;
        let workToDo = true;
        do {
            const oneMinuteFromNow = new Date(Date.now() + 60000);
            continueRunning = oneMinuteFromNow < runUntilDate;

            await fileCopier.runUntil(continueRunning ? oneMinuteFromNow : runUntilDate, bailOutDate);

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
            workToDo = state.workItems.length > 0;

            if (workToDo) {
                logger.info(`${state.workItems.length} work items remaining. Storing FileCopierState`);

                await deleteFileCopierState(jobAssignmentDatabaseId, jobAssignmentHelper.dbTable);
                await saveFileCopierState(state, jobAssignmentDatabaseId, jobAssignmentHelper.dbTable);

                if (!continueRunning) {
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
            }
        } while (continueRunning && workToDo);

        // state no longer needed. finished copying.
        await deleteFileCopierState(jobAssignmentDatabaseId, jobAssignmentHelper.dbTable);

        await Utils.sleep(1000);
        logger.info("Copy was a success, marking job as Completed");
        await jobAssignmentHelper.complete();
    } catch (error) {
        logError(logger, error);
        try {
            await jobAssignmentHelper.fail(new ProblemDetail({
                type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/generic-failure",
                title: "Generic failure",
                detail: error.message
            }));
        } catch (error) {
            logError(logger, error);
        }
    }
}
