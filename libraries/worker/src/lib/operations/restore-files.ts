import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { Locator, McmaException, ProblemDetail, StorageJob } from "@mcma/core";
import { isS3Locator } from "@mcma/aws-s3";
import { HeadObjectCommand, RestoreObjectCommand, StorageClass, Tier } from "@aws-sdk/client-s3";
import { getTableName } from "@mcma/data";

import { RestorePriority, RestoreWorkItem, buildRestoreWorkItemId } from "@local/storage";

import { WorkerContext } from "../worker-context";

export async function restoreFiles(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;
    logger.info(jobInput);

    const files = jobInput.files as Locator[];
    let priority = jobInput.priority as RestorePriority;
    let durationInDays = jobInput.durationInDays as number;

    if (!Array.isArray(files) || files.length === 0) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/missing-input-parameter",
            title: "Missing input parameter",
            detail: "Missing input parameter 'files'",
        }));
        return;
    }

    if (priority && !(priority in RestorePriority)) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/priority-type-not-recognized",
            title: "Provided input priority is not recognized",
            detail: `String value '${priority}' is not one of ${Object.values(RestorePriority).join(", ")}`,
        }));
        return;
    }

    if (durationInDays && (!Number.isSafeInteger(durationInDays) || durationInDays <= 0)) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/duration-in-days-has-invalid-value",
            title: "Provided input durationInDays does not have a positive integer value",
            detail: `Value '${durationInDays}' is not a non-negative integer value`,
        }));
        return;
    }

    for (const file of files) {
        if (!isS3Locator(file)) {
            await jobAssignmentHelper.fail(new ProblemDetail({
                type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/locator-type-not-supported",
                title: "Provided input locator type is not supported",
                detail: `Locator type '${file["@type"]}' is not supported`,
            }));
            return;
        }

        const s3Client = await ctx.storageClientFactory.getS3Client(file.bucket, file.region);
        const headObject = await s3Client.send(new HeadObjectCommand({
            Bucket: file.bucket,
            Key: file.key,
        }));
        logger.info(headObject);

        if (headObject.StorageClass !== StorageClass.GLACIER) {
            await jobAssignmentHelper.fail(new ProblemDetail({
                type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/object-in-unsupported-storage-class",
                title: "Provided object is in unsupported storage class",
                detail: `Object ${file.key} in bucket ${file.bucket} is in storage class ${headObject.StorageClass}.`,
            }));
            return;
        }
    }

    if (!priority) {
        priority = RestorePriority.Low;
    }

    if (!durationInDays) {
        durationInDays = 3;
    }

    for (const file of files) {
        try {
            if (!isS3Locator(file)) {
                throw new McmaException("Should not arrive here");
            }

            const s3Client = await ctx.storageClientFactory.getS3Client(file.bucket, file.region);

            const restoreObject = await s3Client.send(new RestoreObjectCommand({
                Bucket: file.bucket,
                Key: file.key,
                RestoreRequest: {
                    Days: durationInDays,
                    GlacierJobParameters: {
                        Tier: priority === RestorePriority.High ? Tier.Expedited : priority === RestorePriority.Medium ? Tier.Standard : Tier.Bulk
                    }
                }
            }));

            logger.info(restoreObject);
        } catch (error) {
            logger.warn(error);
        }

        const table = await providers.dbTableProvider.get(getTableName());

        const restoreWorkItemId = buildRestoreWorkItemId(file);

        const mutex = table.createMutex({ name: restoreWorkItemId, logger, holder: ctx.requestId });
        await mutex.lock();
        try {
            let restoreWorkItem: RestoreWorkItem = await table.get(restoreWorkItemId);
            if (!restoreWorkItem) {
                restoreWorkItem = new RestoreWorkItem({
                    id: restoreWorkItemId,
                    file,
                });
            }
            restoreWorkItem.jobAssignmentDatabaseIds.push(jobAssignmentHelper.jobAssignmentDatabaseId);

            await table.put(restoreWorkItemId, restoreWorkItem);
        } finally {
            await mutex.unlock();
        }
    }
}
