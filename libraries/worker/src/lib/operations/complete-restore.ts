import { JobStatus, ProblemDetail } from "@mcma/core";
import { getTableName } from "@mcma/data";
import { ProcessJobAssignmentHelper, ProviderCollection, WorkerRequest } from "@mcma/worker";

import { logError } from "@local/storage";

import { WorkerContext } from "../worker-context";

export async function completeRestore(providers: ProviderCollection, workerRequest: WorkerRequest, ctx?: WorkerContext) {
    const jobAssignmentHelper = new ProcessJobAssignmentHelper(
        await providers.dbTableProvider.get(getTableName()),
        providers.resourceManagerProvider.get(),
        workerRequest
    );

    const logger = jobAssignmentHelper.logger;

    try {
        await jobAssignmentHelper.initialize();

        if (jobAssignmentHelper.job.status === JobStatus.Completed || jobAssignmentHelper.job.status === JobStatus.Failed || jobAssignmentHelper.job.status === JobStatus.Canceled) {
            return;
        }

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
