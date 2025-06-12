import { ListObjectsV2Command, ListObjectsV2CommandInput } from "@aws-sdk/client-s3";

import { JobStatus, Locator, McmaException, ProblemDetail, StorageJob, Utils } from "@mcma/core";
import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { getWorkerFunctionId } from "@mcma/worker-invoker";
import { buildS3Url, isS3Locator, S3Locator } from "@mcma/aws-s3";
import { BlobStorageLocator, buildBlobStorageUrl, isBlobStorageLocator } from "@mcma/azure-blob-storage";

import { FileCopier, SourceFile, DestinationFile } from "../operations";
import { WorkerContext } from "../worker-context";
import { logError, saveFileCopierState } from "./utils";

const { MAX_CONCURRENCY, MULTIPART_SIZE } = process.env;

export async function copyFolder(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
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

    const sourceLocator = jobInput.sourceFolder as Locator;
    const targetLocator = jobInput.destinationFolder as Locator;

    const sourceFile: SourceFile = {
        locator: sourceLocator,
        egressUrl: jobInput.sourceEgressUrl,
    };
    const destinationFile: DestinationFile = {
        locator: targetLocator
    };

    const files = await scanSourceFolder(sourceFile, destinationFile, ctx);
    logger.info(files);

    for (const file of files) {
        fileCopier.addFile(file.sourceFile, file.destinationFile);
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

async function scanSourceFolder(sourceFolder: SourceFile, destinationFolder: DestinationFile, ctx: WorkerContext) {
    const files: { sourceFile: SourceFile, destinationFile: DestinationFile }[] = [];

    if (isS3Locator(sourceFolder.locator)) {
        const s3Client = await ctx.storageClientFactory.getS3Client(sourceFolder.locator.bucket);

        const params: ListObjectsV2CommandInput = {
            Bucket: sourceFolder.locator.bucket,
            Prefix: sourceFolder.locator.key,
        };
        do {
            const output = await s3Client.send(new ListObjectsV2Command(params));

            for (const content of output.Contents) {
                const sourceFile: SourceFile = {
                    locator: new S3Locator({
                        url: await buildS3Url(sourceFolder.locator.bucket, content.Key, sourceFolder.locator.region)
                    }),
                    egressUrl: sourceFolder.egressUrl ? sourceFolder.egressUrl + content.Key.substring(sourceFolder.locator.key.length) : undefined,
                };

                let destinationFile: DestinationFile;
                if (isS3Locator(destinationFolder.locator)) {
                    destinationFile = {
                        locator: new S3Locator({ url: await buildS3Url(destinationFolder.locator.bucket, destinationFolder.locator.key + content.Key.substring(sourceFolder.locator.key.length), destinationFolder.locator.region)})
                    }
                } else if (isBlobStorageLocator(destinationFolder.locator)) {
                    destinationFile = {
                        locator: new BlobStorageLocator({ url: buildBlobStorageUrl(destinationFolder.locator.account, destinationFolder.locator.container, destinationFolder.locator.blobName + content.Key.substring(sourceFolder.locator.key.length))})
                    }
                } else {
                    throw new McmaException(`Unsupported target locator type '${destinationFolder.locator["@type"]}'`);
                }

                files.push({ sourceFile, destinationFile });
            }

            params.ContinuationToken = output.NextContinuationToken;
        } while (params.ContinuationToken);

    } else if (isBlobStorageLocator(sourceFolder.locator)) {
        const containerClient = await ctx.storageClientFactory.getContainerClient(sourceFolder.locator.account, sourceFolder.locator.container);

        for await(const blob of containerClient.listBlobsFlat({ prefix: sourceFolder.locator.blobName })) {
            const sourceFile: SourceFile = {
                locator: new BlobStorageLocator({
                    url: buildBlobStorageUrl(sourceFolder.locator.account, sourceFolder.locator.container, blob.name)
                }),
                egressUrl: sourceFolder.egressUrl ? sourceFolder.egressUrl + blob.name.substring(sourceFolder.locator.blobName.length) : undefined,
            };

            let destinationFile: DestinationFile;
            if (isS3Locator(destinationFolder.locator)) {
                destinationFile = {
                    locator: new S3Locator({ url: await buildS3Url(destinationFolder.locator.bucket, destinationFolder.locator.key + blob.name.substring(sourceFolder.locator.blobName.length), destinationFolder.locator.region)})
                }
            } else if (isBlobStorageLocator(destinationFolder.locator)) {
                destinationFile = {
                    locator: new BlobStorageLocator({ url: buildBlobStorageUrl(destinationFolder.locator.account, destinationFolder.locator.container, destinationFolder.locator.blobName + blob.name.substring(sourceFolder.locator.blobName.length))})
                }
            } else {
                throw new McmaException(`Unsupported target locator type '${destinationFolder.locator["@type"]}'`);
            }

            files.push({ sourceFile, destinationFile });
        }
    } else {
        throw new McmaException(`Unsupported source locator type '${sourceFolder.locator["@type"]}'`);
    }

    return files;
}

