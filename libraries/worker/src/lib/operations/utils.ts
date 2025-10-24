import { Locator, McmaException } from "@mcma/core";
import { WorkerContext } from "../worker-context";
import { ListObjectsV2CommandInput, ListObjectsV2Command, StorageClass } from "@aws-sdk/client-s3";
import { buildS3Url, isS3Locator, S3Locator } from "@mcma/aws-s3";
import { BlobStorageLocator, buildBlobStorageUrl, isBlobStorageLocator } from "@mcma/azure-blob-storage";
import { SourceFile, DestinationFile } from "@local/storage";

export async function scanSourceFolderForCopy(sourceFolder: SourceFile, destinationFolder: DestinationFile, ctx: WorkerContext) {
    const files: { sourceFile: SourceFile, destinationFile: DestinationFile }[] = [];

    if (isS3Locator(sourceFolder.locator)) {
        const s3Client = await ctx.storageClientFactory.getS3Client(sourceFolder.locator.bucket);

        const params: ListObjectsV2CommandInput = {
            Bucket: sourceFolder.locator.bucket,
            Prefix: sourceFolder.locator.key,
        };
        do {
            const output = await s3Client.send(new ListObjectsV2Command(params));

            if (Array.isArray(output.Contents)) {
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
                            locator: new S3Locator({ url: await buildS3Url(destinationFolder.locator.bucket, destinationFolder.locator.key + content.Key.substring(sourceFolder.locator.key.length), destinationFolder.locator.region) }),
                            storageClass: destinationFolder.storageClass,
                        }
                    } else if (isBlobStorageLocator(destinationFolder.locator)) {
                        destinationFile = {
                            locator: new BlobStorageLocator({ url: buildBlobStorageUrl(destinationFolder.locator.account, destinationFolder.locator.container, destinationFolder.locator.blobName + content.Key.substring(sourceFolder.locator.key.length)) })
                        }
                    } else {
                        throw new McmaException(`Unsupported target locator type '${destinationFolder.locator["@type"]}'`);
                    }

                    files.push({ sourceFile, destinationFile });
                }
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
                    locator: new S3Locator({ url: await buildS3Url(destinationFolder.locator.bucket, destinationFolder.locator.key + blob.name.substring(sourceFolder.locator.blobName.length), destinationFolder.locator.region)}),
                    storageClass: destinationFolder.storageClass,
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

export async function scanSourceFolderForRestore(folder: Locator, ctx: WorkerContext) {
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
                    if ((content.StorageClass === StorageClass.GLACIER || content.StorageClass === StorageClass.DEEP_ARCHIVE) && !content.RestoreStatus?.IsRestoreInProgress) {
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
