import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { McmaException, StorageJob } from "@mcma/core";
import { WorkerContext } from "../worker-context";

export async function copyFolder(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;

    logger.info(jobInput);

    throw new McmaException("Not Implemented");
}
