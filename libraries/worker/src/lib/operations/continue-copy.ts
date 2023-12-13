import { ProblemDetail, Utils } from "@mcma/core";
import { getTableName } from "@mcma/data";
import { ProcessJobAssignmentHelper, ProviderCollection, WorkerRequest } from "@mcma/worker";
import { getWorkerFunctionId } from "@mcma/worker-invoker";

import { WorkerContext } from "../worker-context";
import { FileCopier } from "../operations";
import { loadFileCopierState, saveFileCopierState } from "./utils";

const { MAX_CONCURRENCY, MULTIPART_SIZE } = process.env;

export async function continueCopy(providers: ProviderCollection, workerRequest: WorkerRequest, ctx?: WorkerContext) {
    const jobAssignmentHelper = new ProcessJobAssignmentHelper(
        await providers.dbTableProvider.get(getTableName()),
        providers.resourceManagerProvider.get(),
        workerRequest
    );

    const logger = jobAssignmentHelper.logger;

    const { fileCopierStateDatabaseIds } = workerRequest.input;

    try {
        await jobAssignmentHelper.initialize();

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

        const state = await loadFileCopierState(fileCopierStateDatabaseIds, jobAssignmentHelper.dbTable);

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

        await fileCopier.runUntil(ctx.timeLimit);

        const error = fileCopier.getError();
        if (error) {
            logger.error("Failing job as copy resulted in a failure");
            logger.error(error);
            await jobAssignmentHelper.fail(new ProblemDetail({
                type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/copy-failure",
                title: "Copy failure",
                detail: error.message,
            }));
            return;
        }

        const state2 = fileCopier.getState();
        if (state2.workItems.length > 0) {
            logger.info(`${state2.workItems.length} work items remaining. Storing FileCopierState`);
            const jobAssignmentDatabaseId = jobAssignmentHelper.jobAssignmentDatabaseId;
            const fileCopierStateDatabaseIds = await saveFileCopierState(state2, jobAssignmentDatabaseId, jobAssignmentHelper.dbTable);

            logger.info(`Invoking worker again`);
            await ctx.workerInvoker.invoke(getWorkerFunctionId(), {
                operationName: "ContinueCopy",
                input: {
                    jobAssignmentDatabaseId,
                    fileCopierStateDatabaseIds,
                },
                tracker: jobAssignmentHelper.workerRequest.tracker
            });
            return;
        }

        await Utils.sleep(1000);
        logger.info("Copy was a success, marking job as Completed");
        await jobAssignmentHelper.complete();
    } catch (error) {
        logger.error(error);
        try {
            await jobAssignmentHelper.fail(new ProblemDetail({
                type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/generic-failure",
                title: "Generic failure",
                detail: error.message
            }));
        } catch (error) {
            logger.error(error);
        }
    }
}

