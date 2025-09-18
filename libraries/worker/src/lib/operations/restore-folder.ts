import { ListObjectsV2Command, ListObjectsV2CommandInput, RestoreObjectCommand, StorageClass, Tier } from "@aws-sdk/client-s3";
import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { Locator, McmaException, ProblemDetail, StorageJob } from "@mcma/core";
import { WorkerContext } from "../worker-context";
import { buildS3Url, isS3Locator, S3Locator } from "@mcma/aws-s3";
import { getTableName } from "@mcma/data";

import { RestorePriority, buildRestoreWorkItemId, RestoreWorkItem } from "@local/storage";

export async function restoreFolder(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;
    logger.info(jobInput);

    const folder = jobInput.folder as Locator;
    let priority = jobInput.priority as RestorePriority;
    let durationInDays = jobInput.durationInDays as number;

    if (!isS3Locator(folder)) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/locator-type-not-supported",
            title: "Provided input locator type is not supported",
            detail: `Locator type '${folder["@type"]}' is not supported`,
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

    if (!priority) {
        priority = RestorePriority.Low;
    }

    if (!durationInDays) {
        durationInDays = 3;
    }

    const files = await scanSourceFolder(folder, ctx);

    if (files.length === 0) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/no-suitable-objects-detected",
            title: "Provided input folder does not contain any suitable objects for restoring",
            detail: `Provided input folder: '${folder.url}'`,
        }));
        return;
    }

    for (const file of files) {
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

async function scanSourceFolder(folder: Locator, ctx: WorkerContext) {
    const files: Locator[] = [];

    if (isS3Locator(folder)) {
        const s3Client = await ctx.storageClientFactory.getS3Client(folder.bucket);

        const params: ListObjectsV2CommandInput = {
            Bucket: folder.bucket,
            Prefix: folder.key,
        };
        do {
            const output = await s3Client.send(new ListObjectsV2Command(params));

            if (Array.isArray(output.Contents)) {
                for (const content of output.Contents) {
                    if (content.StorageClass === StorageClass.GLACIER) {
                        files.push(new S3Locator({
                            url: await buildS3Url(folder.bucket, content.Key, folder.region)
                        }));
                    }
                }
            }

            params.ContinuationToken = output.NextContinuationToken;
        } while (params.ContinuationToken);
    } else {
        throw new McmaException(`Unsupported source locator type '${folder["@type"]}'`);
    }

    return files;
}
