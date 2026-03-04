import { Locator, McmaException } from "@mcma/core";
import { WorkerContext } from "../worker-context";
import { ListObjectsV2CommandInput, ListObjectsV2Command, StorageClass } from "@aws-sdk/client-s3";
import { buildS3Url, isS3Locator, S3Locator } from "@mcma/aws-s3";
import { withRetry } from "@local/storage";

export async function scanSourceFolderForRestore(folder: Locator, ctx: WorkerContext) {
    const files: Locator[] = [];

    if (isS3Locator(folder)) {
        const s3Client = await ctx.storageClientFactory.getS3Client(folder.bucket);

        const params: ListObjectsV2CommandInput = {
            Bucket: folder.bucket,
            Prefix: folder.key,
        };
        do {
            const output = await withRetry(() => s3Client.send(new ListObjectsV2Command(params)));

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
