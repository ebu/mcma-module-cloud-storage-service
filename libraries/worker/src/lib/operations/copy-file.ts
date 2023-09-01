import { default as axios } from "axios";

import { Readable, PassThrough } from "stream";

import { Locator, StorageJob } from "@mcma/core";
import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { WorkerContext } from "..";
import { isBlobStorageLocator, isS3Locator } from "./utils";
import { CopyObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { BlobSASPermissions } from "@azure/storage-blob";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export async function copyFile(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<StorageJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;

    logger.info(jobInput);

    const sourceFile = jobInput.sourceFile as Locator;
    const targetFile = jobInput.targetFile as Locator;

    if (isS3Locator(targetFile)) {
        logger.info("Target file is S3Locator");
        const s3Client = await ctx.storageClientFactory.getS3Client(targetFile.bucket);

        if (isS3Locator(sourceFile)) {
            // if source is on S3 try s3 copy by using target credentials
            logger.info("Source is S3 Locator. Trying S3 copy");
            try {
                // TODO replace with multipart copy
                await s3Client.send(new CopyObjectCommand({
                    Bucket: targetFile.bucket,
                    Key: targetFile.key,
                    CopySource: `/${sourceFile.bucket}/${sourceFile.key}`
                }));

                await jobAssignmentHelper.complete();
                return;
            } catch (error) {
                logger.warn(error);
                logger.warn("Failed to do S3 copy. Trying regular copy");
            }
        }

        try {
            logger.info("Checking if file is publicly available");
            // if source is publicly available we can copy from there.
            await axios.head(sourceFile.url);
            logger.info("File is publicly available. Copying from public URL");

            const passThrough = new PassThrough();

            const response = await axios.get(sourceFile.url, { responseType: "stream" });
            response.data.pipe(passThrough);

            const upload = new Upload({
                client: s3Client,
                params: {
                    Bucket: targetFile.bucket,
                    Key: targetFile.key,
                    Body: passThrough
                }
            });

            await upload.done();

            await jobAssignmentHelper.complete();
            return;
        } catch (error) {
            logger.warn(error);
            logger.warn("File is not publicly available");
        }

        if (isS3Locator(sourceFile)) {
            logger.info("Source file is s3 but s3 copy wasn't possible. Try copying manually");

            const passThrough = new PassThrough();

            const sourceS3Client = await ctx.storageClientFactory.getS3Client(sourceFile.bucket);
            const output = await sourceS3Client.send(new GetObjectCommand({
                Bucket: sourceFile.bucket,
                Key: sourceFile.key

            }));
            (output.Body as Readable).pipe(passThrough);

            const upload = new Upload({
                client: s3Client,
                params: {
                    Bucket: targetFile.bucket,
                    Key: targetFile.key,
                    Body: passThrough
                }
            });

            await upload.done();

            await jobAssignmentHelper.complete();
            return;
        }

        if (isBlobStorageLocator(sourceFile)) {
            logger.info("Source file is blob storage");
            const containerClient = await ctx.storageClientFactory.getContainerClient(sourceFile.account, sourceFile.container);

            const passThrough = new PassThrough();

            const blobClient = containerClient.getBlockBlobClient(sourceFile.blobName);
            const response = await blobClient.download();

            response.readableStreamBody.pipe(passThrough);

            const upload = new Upload({
                client: s3Client,
                params: {
                    Bucket: targetFile.bucket,
                    Key: targetFile.key,
                    Body: passThrough
                }
            });

            await upload.done();

            await jobAssignmentHelper.complete();
            return;
        }
    } else if (isBlobStorageLocator(targetFile)) {
        logger.info("Target file is BlobStorageLocator");

        const containerClient = await ctx.storageClientFactory.getContainerClient(targetFile.account, targetFile.container);

        const blobClient = containerClient.getBlockBlobClient(targetFile.blobName);

        if (isBlobStorageLocator(sourceFile)) {
            logger.info("Source file is BlobStorageLocator. Let's try copy the storage blob");
            try {
                const sourceContainerClient = await ctx.storageClientFactory.getContainerClient(sourceFile.account, sourceFile.container);

                const sourceBlobClient = sourceContainerClient.getBlockBlobClient(sourceFile.blobName);

                const sasUrl = await sourceBlobClient.generateSasUrl({
                    expiresOn: new Date(Date.now() + 3600000),
                    permissions: BlobSASPermissions.from({ read: true })
                });

                const result = await blobClient.syncCopyFromURL(sasUrl);

                logger.info(result);

                await jobAssignmentHelper.complete();
                return;
            } catch (error) {
                logger.warn(error);
                logger.warn("Failed to copy storage blob");
            }
        }

        try {
            logger.info("Checking if file is publicly available");
            // if source is publicly available we can copy from there.
            await axios.head(sourceFile.url);
            logger.info("File is publicly available. Copying from public URL");

            await blobClient.syncCopyFromURL(sourceFile.url);

            await jobAssignmentHelper.complete();
            return;
        } catch (error) {
            logger.warn(error);
            logger.warn("File is not publicly available");
        }

        if (isS3Locator(sourceFile)) {
            logger.info("Source is S3 Locator");
            const s3Client = await ctx.storageClientFactory.getS3Client(sourceFile.bucket);

            const command = new GetObjectCommand({
                Bucket: sourceFile.bucket,
                Key: sourceFile.key,
            });

            const url = await getSignedUrl(s3Client, command, { expiresIn: 3600 });

            await blobClient.syncCopyFromURL(url);

            await jobAssignmentHelper.complete();
            return;
        }
    }

    await jobAssignmentHelper.fail({
        type: "uri://mcma.ebu.ch/rfc7807/cloud-storage-service/not-supported",
        title: "Copying file from generic non-public Locator is not supported.",
    });
}
