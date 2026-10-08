import { JobStatus, ProblemDetail, Utils } from "@mcma/core";
import { getTableName } from "@mcma/data";
import { ProcessJobAssignmentHelper, ProviderCollection, WorkerRequest } from "@mcma/worker";
import { getWorkerFunctionId } from "@mcma/worker-invoker";

import { FileCopier, FileCopierState, logError } from "@local/storage";

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

        const jobInput = jobAssignmentHelper.jobInput;

        if (jobAssignmentHelper.job.status === JobStatus.Completed || jobAssignmentHelper.job.status === JobStatus.Failed || jobAssignmentHelper.job.status === JobStatus.Canceled) {
            return;
        }

        const getS3Client = async (bucket: string, region?: string) => ctx.storageClientFactory.getS3Client(bucket, region);
        const getContainerClient = async (account: string, container: string) => ctx.storageClientFactory.getContainerClient(account, container);

        const progressUpdate = async (filesTotal: number, filesCopied: number, bytesTotal: number, bytesCopied: number) => {
            if (bytesTotal > 0) {
                const progress = Math.round((bytesCopied / bytesTotal * 100 + Number.EPSILON) * 10) / 10;
                if (typeof jobAssignmentHelper.jobAssignment.progress !== "number" || Math.abs(jobAssignmentHelper.jobAssignment.progress - progress) >= 0.1) {
                    logger.info(`${progress}%`);
                    await jobAssignmentHelper.updateJobAssignment(jobAssigment => jobAssigment.progress = progress, true);
                }
            }
        };

        const runUntilDate = new Date(ctx.functionTimeLimit.getTime() - 60000);
        const bailOutDate = new Date(ctx.functionTimeLimit.getTime() - 10000);
        const abortTimeout = ctx.functionTimeLimit.getTime() - Date.now() - 30000;
        const checkpointUpdate = async (state: FileCopierState) => {
            logger.debug(`Saving live FileCopier checkpoint with ${state.workItems.length} unfinished work items`);
            await ctx.saveFileCopierState(jobAssignmentDatabaseId, state);
        };

        const pathFilter = jobInput.pathFilter as string;

        const fileCopier = new FileCopier({
            maxConcurrency: Number.parseInt(MAX_CONCURRENCY),
            multipartSize: Number.parseInt(MULTIPART_SIZE),
            pathFilter,
            logger,
            getS3Client,
            getContainerClient,
            progressUpdate,
            checkpointUpdate,
            axiosConfig: {
                signal: AbortSignal.timeout(abortTimeout)
            }
        });

        {
            const state = await ctx.loadFileCopierState(jobAssignmentDatabaseId);

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
            await fileCopier.setState(state);
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

        const state = await fileCopier.getState();
        if (state.workItems.length > 0) {
            logger.info(`${state.workItems.length} work items remaining. Storing FileCopierState`);
            await ctx.saveFileCopierState(jobAssignmentDatabaseId, state);

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

        // state no longer needed. finished copying.
        await ctx.deleteFileCopierState(jobAssignmentDatabaseId);

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
