import { Locator, Logger, McmaException } from "@mcma/core";
import { GetObjectCommand, HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { BlobSASPermissions, ContainerClient } from "@azure/storage-blob";

import { ObjectData, SourceFile, TargetFile } from "./model";
import { isBlobStorageLocator, isS3Locator } from "./utils";
import { copyFileFromUrlToS3 } from "./copy-file-from-url-to-s3";
import { copyFileFromS3ToS3 } from "./copy-file-from-s3-to-s3";
import { AxiosRequestConfig, default as axios } from "axios";


export async function doCopyFile(logger: Logger, sourceFile: SourceFile, targetFile: TargetFile, getS3Client: (bucket: string) => Promise<S3Client>, getContainerClient: (account: string, container: string) => Promise<ContainerClient>, axiosConfig?: AxiosRequestConfig) {

    logger.info(`Copying file '${sourceFile.locator.url}' to '${targetFile.locator.url}'`);
    const sourceObjectData = await getObjectData(logger, sourceFile.locator, getS3Client, getContainerClient, axiosConfig);
    if (!sourceObjectData.size) {
        throw new McmaException("Failed to obtain source object data");
    }

    // verify if target already contains file with same name, file size and checksum
    const targetObjectData = await getObjectData(logger, targetFile.locator, getS3Client, getContainerClient, axiosConfig);
    if (targetObjectData.size) {
        if (sourceObjectData.etag && targetObjectData.etag && targetObjectData.etag === sourceObjectData.etag) {
            logger.info(`Target file already exists with same MD5 checksum: '${targetObjectData.etag}'`);
            return;
        }

        if (targetObjectData.size === sourceObjectData.size && targetObjectData.lastModified > sourceObjectData.lastModified) {
            logger.info(`Target file already exists with same size : '${targetObjectData.size}'`);
            return;
        }
    }

    logger.info(`Coping file with size: ${sourceObjectData.size}`);
    const t1 = Date.now();
    try {
        if (sourceFile.alternateUrl) {
            logger.info(`Copying from alternate URL ${sourceFile.alternateUrl}`);

            let url = sourceFile.alternateUrl;
            if (!url.endsWith("/")) {
                url += "/";
            }
            if (isS3Locator(sourceFile.locator)) {
                url = sourceFile.locator.key;
            } else if (isBlobStorageLocator(sourceFile.locator)) {
                url = sourceFile.locator.blobName;
            } else {
                throw new McmaException(`Unrecognized locator type '${sourceFile.locator["@type"]}'`);
            }

            let headers: { [key: string]: string } = {};
            switch (sourceFile.alternateAuthType) {
                case "McmaApiKey":
                    // TODO: add 'x-mcma-api-key' to headers
                    break;
            }

            if (isS3Locator(targetFile.locator)) {
                logger.info("Copying file from alternate URL to S3");
                const s3Client = await getS3Client(targetFile.locator.bucket);
                await copyFileFromUrlToS3(logger, url, sourceObjectData, targetFile.locator, s3Client, Object.assign({}, axiosConfig, { headers }));
                return;
            } else if (isBlobStorageLocator(targetFile.locator)) {
                logger.info("Copying file from alternate URL to BlobStorage");
                const containerClient = await getContainerClient(targetFile.locator.account, targetFile.locator.container);
                const blobClient = containerClient.getBlockBlobClient(targetFile.locator.blobName);
                const response = await blobClient.beginCopyFromURL(url, {});
                await response.pollUntilDone();
                return;
            } else {
                throw new McmaException(`Unrecognized locator type '${targetFile.locator["@type"]}'`);
            }
        }

        if (isS3Locator(targetFile.locator)) {
            logger.info("Target file is S3Locator");
            const s3Client = await getS3Client(targetFile.locator.bucket);

            if (isS3Locator(sourceFile.locator)) {
                // if source is on S3 try s3 copy by using target credentials
                logger.info("Source is S3 Locator. Trying S3 copy");
                try {
                    await copyFileFromS3ToS3(logger, sourceFile.locator, sourceObjectData, targetFile.locator, s3Client);
                    return;
                } catch (error) {
                    logger.warn(error);
                    logger.warn("Failed to do S3 copy. Trying regular copy");
                }
            }

            try {
                // if source is publicly available we can copy from there.
                logger.info("Trying regular copy from directly provided URL");
                await copyFileFromUrlToS3(logger, sourceFile.locator.url, sourceObjectData, targetFile.locator, s3Client, axiosConfig);
                return;
            } catch (error) {
                logger.warn(error);
                logger.warn("File is not publicly available");
            }

            if (isS3Locator(sourceFile.locator)) {
                logger.info("Copying using signed S3 URL");
                const sourceS3Client = await getS3Client(sourceFile.locator.bucket);

                const command = new GetObjectCommand({
                    Bucket: sourceFile.locator.bucket,
                    Key: sourceFile.locator.key,
                });

                const url = await getSignedUrl(sourceS3Client, command, { expiresIn: 3600 });

                await copyFileFromUrlToS3(logger, url, sourceObjectData, targetFile.locator, s3Client, axiosConfig);
                return;
            } else if (isBlobStorageLocator(sourceFile.locator)) {
                logger.info("Copying using BlobStorage Sas URL");

                const sourceContainerClient = await getContainerClient(sourceFile.locator.account, sourceFile.locator.container);
                const sourceBlobClient = sourceContainerClient.getBlockBlobClient(sourceFile.locator.blobName);
                const url = await sourceBlobClient.generateSasUrl({
                    expiresOn: new Date(Date.now() + 3600000),
                    permissions: BlobSASPermissions.from({ read: true })
                });

                await copyFileFromUrlToS3(logger, url, sourceObjectData, targetFile.locator, s3Client, axiosConfig);
                return;
            }
        } else if (isBlobStorageLocator(targetFile.locator)) {
            logger.info("Target file is BlobStorageLocator");

            const containerClient = await getContainerClient(targetFile.locator.account, targetFile.locator.container);
            const blobClient = containerClient.getBlockBlobClient(targetFile.locator.blobName);

            if (isBlobStorageLocator(sourceFile.locator)) {
                logger.info("Source file is BlobStorageLocator. Let's try copy the storage blob");
                try {
                    const sourceContainerClient = await getContainerClient(sourceFile.locator.account, sourceFile.locator.container);
                    const sourceBlobClient = sourceContainerClient.getBlockBlobClient(sourceFile.locator.blobName);
                    const url = await sourceBlobClient.generateSasUrl({
                        expiresOn: new Date(Date.now() + 3600000),
                        permissions: BlobSASPermissions.from({ read: true })
                    });

                    const response = await blobClient.beginCopyFromURL(url);
                    await response.pollUntilDone();
                    return;
                } catch (error) {
                    logger.warn(error);
                    logger.warn("Failed to copy storage blob");
                }
            }

            try {
                logger.info("Checking if file is publicly available");
                // if source is publicly available we can copy from there.
                await axios.head(sourceFile.locator.url);
                logger.info("File is publicly available. Copying from public URL");

                const response = await blobClient.beginCopyFromURL(sourceFile.locator.url);
                await response.pollUntilDone();
                return;
            } catch (error) {
                logger.warn(error);
                logger.warn("File is not publicly available");
            }

            if (isS3Locator(sourceFile.locator)) {
                logger.info("Source is S3 Locator");
                const s3Client = await getS3Client(sourceFile.locator.bucket);

                const command = new GetObjectCommand({
                    Bucket: sourceFile.locator.bucket,
                    Key: sourceFile.locator.key,
                });
                const url = await getSignedUrl(s3Client, command, { expiresIn: 3600 });

                const response = await blobClient.beginCopyFromURL(url);
                await response.pollUntilDone();
                return;
            }
        }
    } finally {
        const t2 = Date.now();
        logger.info(`Copy done in ${(t2 - t1) / 1000} s`);
    }
}

async function getObjectData(logger: Logger, locator: Locator, getS3Client: (bucket: string) => Promise<S3Client>, getContainerClient: (account: string, container: string) => Promise<ContainerClient>, axiosConfig?: AxiosRequestConfig): Promise<ObjectData> {
    const objectData: ObjectData = {};

    let useAxiosOnUrl = true;

    try {
        if (isS3Locator(locator)) {
            const s3Client = await getS3Client(locator.bucket);
            try {
                useAxiosOnUrl = false;
                const headOutput = await s3Client.send(new HeadObjectCommand({ Bucket: locator.bucket, Key: locator.key }));

                objectData.etag = headOutput.ETag;
                objectData.size = headOutput.ContentLength;
                objectData.lastModified = headOutput.LastModified;
                objectData.contentType = headOutput.ContentType;
            } catch (error) {
                logger.info(error.message);
                logger.warn(`Failed to obtain object data for '${locator.key}'. It may not exist`);
            }
        } else if (isBlobStorageLocator(locator)) {
            const containerClient = await getContainerClient(locator.account, locator.container);
            try {
                useAxiosOnUrl = false;
                const blobClient = containerClient.getBlobClient(locator.blobName);
                const propertiesResponse = await blobClient.getProperties();

                objectData.etag = propertiesResponse.etag;
                objectData.contentType = propertiesResponse.contentType;
                objectData.size = propertiesResponse.contentLength;
                objectData.lastModified = propertiesResponse.lastModified;

                logger.info(objectData.etag);
            } catch (error) {
                logger.info(error.message);
                logger.warn(`Failed to obtain object data for '${locator.blobName}'. It may not exist`);
            }
        }
    } catch (error) {}

    // in case we were not able to obtain metadata with SDK clients try a HEAD request on the provided URL.
    if (useAxiosOnUrl) {
        try {
            const response = await axios.head(locator.url, axiosConfig);
            objectData.etag = response.headers["etag"];
            objectData.contentType = response.headers["content-type"];
            objectData.size = Number.parseInt(response.headers["content-length"]);
            objectData.lastModified = new Date(response.headers["last-modified"]);
        } catch (error) {
            logger.info(error.message);
            logger.warn(`Failed to obtain object data for '${locator.url}'. It may not exist`);
        }
    }

    return objectData;
}
