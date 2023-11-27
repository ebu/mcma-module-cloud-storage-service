import { AxiosRequestConfig, default as axios } from "axios";
import { randomBytes } from "crypto";
import {
    CompleteMultipartUploadCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    GetObjectCommand,
    HeadObjectCommand,
    PutObjectCommand,
    S3Client,
    UploadPartCommand,
    UploadPartCopyCommand
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { BlobSASPermissions, ContainerClient } from "@azure/storage-blob";

import { Logger, McmaException, Utils } from "@mcma/core";
import { SecretsProvider } from "@mcma/secrets";
import { getApiKeySecretId } from "@mcma/client";

import { ActiveWorkItem, MultipartSegment, SourceFile, TargetFile, WorkItem, WorkType } from "./model";
import { isBlobStorageLocator, isS3Locator } from "./utils";

const MAX_CONCURRENCY = 32;
const MULTIPART_SIZE = 67108864; // 64MiB

export interface FileCopierConfig {
    maxConcurrency?: number;
    multipartSize?: number;
    getS3Client: (bucket: string, region?: string) => Promise<S3Client>;
    getContainerClient: (account: string, container: string) => Promise<ContainerClient>;
    progressUpdate?: (filesTotal: number, filesCopied: number, bytesTotal: number, bytesCopied: number) => Promise<void>;
    axiosConfig?: AxiosRequestConfig;
    logger: Logger;
    apiKey?: string;
    apiKeySecretId?: string;
    secretsProvider?: SecretsProvider;
}

export class FileCopier {
    private readonly queuedWorkItems: WorkItem[];
    private readonly activeWorkItems: ActiveWorkItem[];
    private readonly logger: Logger;

    private maxConcurrency: number;
    private multipartSize: number;
    private filesTotal: number;
    private filesCopied: number;
    private bytesTotal: number;
    private bytesCopied: number;
    private processing: boolean;
    private running: boolean;
    private error: Error;

    constructor(private config: FileCopierConfig) {
        this.queuedWorkItems = [];
        this.activeWorkItems = [];
        this.filesTotal = 0;
        this.filesCopied = 0;
        this.bytesTotal = 0;
        this.bytesCopied = 0;
        this.logger = config.logger;
        this.maxConcurrency = this.config.maxConcurrency > 0 && this.config.maxConcurrency < 64 ? this.config.maxConcurrency : MAX_CONCURRENCY;
        this.multipartSize = this.config.multipartSize >= 5242880 && this.config.multipartSize <= 4194304000 ? this.config.multipartSize : MULTIPART_SIZE; // min limit AWS and max limit blob storage
    }

    public setWorkItems(workItems: WorkItem[]) {
        const copyWorkItems = JSON.parse(JSON.stringify(workItems)) as WorkItem[];

        const multipartCompleteWorkItems = copyWorkItems.filter(w => w.type === WorkType.MultipartComplete);
        const multipartCompleteWorkItemsMap: { [key: string]: WorkItem } = {};
        for (const workItem of multipartCompleteWorkItems) {
            multipartCompleteWorkItemsMap[workItem.sourceFile.locator.url] = workItem;
        }

        for (const workItem of copyWorkItems) {
            if (workItem.type === WorkType.MultipartSegment) {
                const multipartCompleteWorkItem = multipartCompleteWorkItemsMap[workItem.sourceFile.locator.url];
                if (!multipartCompleteWorkItem) {
                    throw new McmaException("Incomplete work items list");
                }
                const idx = multipartCompleteWorkItem.multipartData.segments.findIndex(s => s.partNumber === workItem.multipartData.segment.partNumber);
                multipartCompleteWorkItem.multipartData.segments[idx] = workItem.multipartData.segment;
            }
        }

        this.queuedWorkItems.push(...copyWorkItems);
    }

    public getWorkItems(): WorkItem[] {
        return JSON.parse(JSON.stringify(this.queuedWorkItems)) as WorkItem[];
    }

    public addFile(sourceFile: SourceFile, targetFile: TargetFile) {
        this.queuedWorkItems.push({
            type: WorkType.Prepare,
            sourceFile,
            targetFile,
            retries: 0,
        });
    }

    public async runUntil(date: Date) {
        this.logger.info("FileCopier.runUntil() - Start");
        if (this.running || this.processing) {
            throw new McmaException("Can't invoke method FileCopier.runUntil if it's already invoked");
        }

        this.running = true;
        try {
            this.maxConcurrency = this.config.maxConcurrency > 0 && this.config.maxConcurrency < 64 ? this.config.maxConcurrency : MAX_CONCURRENCY;

            this.logger.info("FileCopier.runUntil() - Starting process thread");

            this.process().then();

            this.logger.info("FileCopier.runUntil() - Wait until timeout, finished work, or an error");

            while (date > new Date() && (this.activeWorkItems.length > 0 || this.queuedWorkItems.length > 0) && !this.error) {
                await Utils.sleep(1000);
                if (this.config.progressUpdate) {
                    await this.config.progressUpdate(this.filesTotal, this.filesCopied, this.bytesTotal, this.bytesCopied);
                }
            }

            if (!(date > new Date())) {
                this.logger.info("FileCopier.runUntil() - Timeout reached");
            } else if (this.error) {
                this.logger.info("FileCopier.runUntil() - Error occurred");
            } else {
                this.logger.info("FileCopier.runUntil() - Finished work");
            }

            this.maxConcurrency = 0;

            if (this.processing && this.activeWorkItems.length > 0) {
                this.logger.info("FileCopier.runUntil() - Wait for active work items to finish");

                while (this.activeWorkItems.length > 0) {
                    await Utils.sleep(1000);
                    if (this.config.progressUpdate) {
                        await this.config.progressUpdate(this.filesTotal, this.filesCopied, this.bytesTotal, this.bytesCopied);
                    }
                }
            }
        } finally {
            this.running = false;
        }

        if (this.processing) {
            this.logger.info("FileCopier.runUntil() - Wait for process thread to stop");

            while (this.processing) {
                await Utils.sleep(250);
            }
        }

        this.logger.info("FileCopier.runUntil() - End");
    }

    public getError() {
        return this.error;
    }

    private async process() {
        this.logger.info("FileCopier.process() - Begin");
        this.processing = true;
        try {
            while (this.running && (this.queuedWorkItems.length > 0 || this.activeWorkItems.length > 0)) {

                // if we have active work items AND we have reached either max concurrency or an empty queue we need to wait for active work items to complete
                if (this.activeWorkItems.length > 0 && (this.activeWorkItems.length >= this.maxConcurrency || this.queuedWorkItems.length === 0)) {
                    const activeWorkItem = await Promise.race(this.activeWorkItems.map(activeWorkItem =>
                        activeWorkItem.promise.then(result => {
                            activeWorkItem.result = result;
                            return activeWorkItem;
                        }).catch(error => {
                            activeWorkItem.error = error;
                            return activeWorkItem;
                        })
                    ));
                    const idx = this.activeWorkItems.indexOf(activeWorkItem);
                    this.activeWorkItems.splice(idx, 1);

                    switch (activeWorkItem.workItem.type) {
                        case WorkType.Prepare:
                            this.finishWorkItemPrepare(activeWorkItem);
                            break;
                        case WorkType.Single:
                            this.finishWorkItemSingle(activeWorkItem);
                            break;
                        case WorkType.MultipartStart:
                            this.finishWorkItemMultipartStart(activeWorkItem);
                            break;
                        case WorkType.MultipartSegment:
                            this.finishWorkItemMultipartSegment(activeWorkItem);
                            break;
                        case WorkType.MultipartComplete:
                            this.finishWorkItemMultipartComplete(activeWorkItem);
                            break;
                    }
                }

                // if we have queued WorkItems and we have have not yet reached max concurrency we'll process next work item.
                if (this.queuedWorkItems.length > 0 && this.activeWorkItems.length < this.maxConcurrency) {
                    const workItem = this.queuedWorkItems.shift();
                    switch (workItem.type) {
                        case WorkType.Prepare:
                            this.processWorkItemPrepare(workItem);
                            break;
                        case WorkType.Single:
                            this.processWorkItemSingle(workItem);
                            break;
                        case WorkType.MultipartStart:
                            this.processWorkItemMultipartStart(workItem);
                            break;
                        case WorkType.MultipartSegment:
                            this.processWorkItemMultipartSegment(workItem);
                            break;
                        case WorkType.MultipartComplete:
                            this.processWorkItemMultipartComplete(workItem);
                            break;
                    }
                } else {
                    await Utils.sleep(250);
                }
            }
        } catch (error) {
            this.logger.error("FileCopier.process() - Error caught:");
            this.logger.error(error);
            this.error = error;
        } finally {
            this.processing = false;
            this.logger.info("FileCopier.process() - End");
        }
    }

    /***
     * The goal of work item prepare is to figure out how the source should be read (URL or S3 Copy),
     * to obtain object metadata, and to determine whether the target object already exists
     ***/
    processWorkItemPrepare(workItem: WorkItem) {
        this.logger.info(`FileCopier.processWorkItemPrepare() - ${workItem.sourceFile.locator.url}`);

        const promise = new Promise<any>(async (resolve, reject) => {
            try {
                let sourceUrl: string = undefined;
                let sourceHeaders: { [key: string]: string } = undefined;
                let contentLength: number = undefined;
                let contentType: string = undefined;
                let lastModified: Date = undefined;

                if (workItem.sourceFile.egressUrl) {
                    // in case we have an egressUrl, we will use that one.
                    sourceUrl = workItem.sourceFile.egressUrl;
                    if (workItem.sourceFile.egressAuthType === "McmaApiKey") {
                        // in case we have McmaApiKey authentication add the correct header
                        if (!this.config.apiKey) {
                            if (!this.config.secretsProvider) {
                                throw new McmaException("FileCopierConfig misses either property 'apiKey' or 'secretsProvider'");
                            }
                            if (!this.config.apiKeySecretId) {
                                this.config.apiKeySecretId = getApiKeySecretId();
                            }

                            this.config.apiKey = await this.config.secretsProvider.get(this.config.apiKeySecretId);
                        }
                        sourceHeaders = {};
                        sourceHeaders["x-mcma-api-key"] = this.config.apiKey;
                    }
                } else {
                    if (isS3Locator(workItem.sourceFile.locator) && isS3Locator(workItem.targetFile.locator)) {
                        this.logger.info("Source AND Target are S3");
                        // if both source and target are S3 try to see if we can access the source with target credentials so we can do s3 copy
                        const s3Client = await this.config.getS3Client(workItem.targetFile.locator.bucket, workItem.sourceFile.locator.region);

                        try {
                            const commandOutput = await s3Client.send(new HeadObjectCommand({
                                Bucket: workItem.sourceFile.locator.bucket,
                                Key: workItem.sourceFile.locator.key,
                            }));

                            contentLength = commandOutput.ContentLength;
                            contentType = commandOutput.ContentType;
                            lastModified = commandOutput.LastModified;
                        } catch (error) {
                            this.logger.error(error);
                            this.logger.info("Source AND Target are S3 - FAILED head request ");
                        }
                    }

                    // for all other cases we'll need a URL as a source
                    if (!contentLength) {
                        // first check if locator URL is publicly readable.
                        try {
                            const headResponse = await axios.head(workItem.sourceFile.locator.url, this.config.axiosConfig);
                            sourceUrl = workItem.sourceFile.locator.url;
                            contentLength = Number.parseInt(headResponse.headers["content-length"]);
                            contentType = headResponse.headers["content-type"];
                            lastModified = new Date(headResponse.headers["last-modified"]);
                        } catch {}
                    }

                    if (!contentLength) {
                        // if not we'll create a signed URL
                        if (isS3Locator(workItem.sourceFile.locator)) {
                            this.logger.info("Creating Signed URL for source S3 Client ");
                            // if we can't do S3 Copy we'll have to generate a signed URL
                            const sourceS3Client = await this.config.getS3Client(workItem.sourceFile.locator.bucket);
                            const command = new GetObjectCommand({
                                Bucket: workItem.sourceFile.locator.bucket,
                                Key: workItem.sourceFile.locator.key,
                            });
                            sourceUrl = await getSignedUrl(sourceS3Client, command, { expiresIn: 12 * 3600 });
                        } else if (isBlobStorageLocator(workItem.sourceFile.locator)) {
                            const sourceContainerClient = await this.config.getContainerClient(workItem.sourceFile.locator.account, workItem.sourceFile.locator.container);
                            const sourceBlobClient = sourceContainerClient.getBlockBlobClient(workItem.sourceFile.locator.blobName);
                            sourceUrl = await sourceBlobClient.generateSasUrl({
                                expiresOn: new Date(Date.now() + 12 * 3600000),
                                permissions: BlobSASPermissions.from({ read: true })
                            });
                        }
                    }
                }

                // in case we didn't fetch yet the object metadata through operations above we'll do a get request for first byte on the sourceURL (head request does not work on aws v4 signed urls)
                if (!contentLength) {
                    const headers = Object.assign({}, this.config.axiosConfig?.headers, sourceHeaders, { range: "bytes=0-0" });
                    const axiosConfig = Object.assign({}, this.config.axiosConfig, { headers });

                    const headResponse = await axios.get(sourceUrl, axiosConfig);
                    contentLength = Number.parseInt(headResponse.headers["content-range"].split("/")[1]);
                    contentType = headResponse.headers["content-type"];
                    lastModified = new Date(headResponse.headers["last-modified"]);

                    if (Number.isNaN(contentLength)) {
                        throw new McmaException("Failed to obtain content length");
                    }
                }

                // now we check if target already exists and has the same contentLength and a newer lastModified date
                // this means it's most likely an exact copy.
                let targetContentLength: number = undefined;
                let targetContentType: string = undefined;
                let targetLastModified: Date = undefined;
                if (isS3Locator(workItem.targetFile.locator)) {
                    const s3Client = await this.config.getS3Client(workItem.targetFile.locator.bucket);
                    try {
                        const headOutput = await s3Client.send(new HeadObjectCommand({
                            Bucket: workItem.targetFile.locator.bucket,
                            Key: workItem.targetFile.locator.key
                        }));

                        targetContentLength = headOutput.ContentLength;
                        targetContentType = headOutput.ContentType;
                        targetLastModified = headOutput.LastModified;
                    } catch (error) {}
                } else if (isBlobStorageLocator(workItem.targetFile.locator)) {
                    const containerClient = await this.config.getContainerClient(workItem.targetFile.locator.account, workItem.targetFile.locator.container);
                    try {
                        const blobClient = containerClient.getBlobClient(workItem.targetFile.locator.blobName);
                        const propertiesResponse = await blobClient.getProperties();

                        targetContentLength = propertiesResponse.contentLength;
                        targetContentType = propertiesResponse.contentType;
                        targetLastModified = propertiesResponse.lastModified;
                    } catch (error) {}
                }

                let skip = contentLength === targetContentLength && contentType === targetContentType && lastModified < targetLastModified;

                resolve({ sourceUrl, sourceHeaders, contentLength, contentType, lastModified, skip });
            } catch (error) {
                reject(error);
            }
        });

        this.activeWorkItems.push({
            workItem,
            promise,
        });
    }

    finishWorkItemPrepare(activeWorkItem: ActiveWorkItem) {
        this.logger.info(`FileCopier.finishWorkItemPrepare() - ${activeWorkItem.workItem.sourceFile.locator.url}`);

        if (activeWorkItem.error) {
            this.logger.error(activeWorkItem.error);
            if (activeWorkItem.workItem.retries++ < 1) {
                this.queuedWorkItems.push(activeWorkItem.workItem);
            } else {
                throw activeWorkItem.error;
            }
        } else {
            const { sourceUrl, sourceHeaders, contentLength, contentType, lastModified, skip } = activeWorkItem.result;

            if (skip) {
                this.logger.info(`${activeWorkItem.workItem.sourceFile.locator.url} already present on target location`);
                return;
            }

            this.filesTotal++;
            this.bytesTotal += contentLength;

            const workItem = Object.assign({}, activeWorkItem.workItem, {
                type: (contentLength > this.multipartSize) ? WorkType.MultipartStart : WorkType.Single,
                retries: 0,
                sourceUrl,
                sourceHeaders,
                contentLength,
                contentType,
                lastModified,
            });
            this.queuedWorkItems.push(workItem);
        }
    }

    processWorkItemSingle(workItem: WorkItem) {
        this.logger.info(`FileCopier.processWorkItemSingle() - ${workItem.sourceFile.locator.url}`);
        const promise = new Promise<void>(async (resolve, reject) => {
            try {
                if (isS3Locator(workItem.targetFile.locator)) {
                    const s3Client = await this.config.getS3Client(workItem.targetFile.locator.bucket);
                    if (!workItem.sourceUrl && isS3Locator(workItem.sourceFile.locator)) {
                        await s3Client.send(new CopyObjectCommand({
                            Bucket: workItem.targetFile.locator.bucket,
                            Key: workItem.targetFile.locator.key,
                            CopySource: encodeURI(workItem.sourceFile.locator.bucket + "/" + workItem.sourceFile.locator.key),
                            ContentType: workItem.contentType,
                        }));
                    } else {
                        const headers = Object.assign({}, this.config.axiosConfig?.headers, workItem.sourceHeaders);
                        const axiosConfig = Object.assign({}, this.config.axiosConfig, { headers, responseType: "arraybuffer" });

                        this.logger.info(`Downloading ${workItem.sourceFile.locator.url} from ${workItem.sourceUrl}`);
                        const response = await axios.get(workItem.sourceUrl, axiosConfig);
                        this.logger.info(`Uploading ${workItem.sourceFile.locator.url} to ${workItem.targetFile.locator.url}`);

                        await s3Client.send(new PutObjectCommand({
                            Bucket: workItem.targetFile.locator.bucket,
                            Key: workItem.targetFile.locator.key,
                            Body: response.data,
                            ContentType: workItem.contentType
                        }));
                    }
                } else if (isBlobStorageLocator(workItem.targetFile.locator)) {
                    const containerClient = await this.config.getContainerClient(workItem.targetFile.locator.account, workItem.targetFile.locator.container);
                    const blobClient = containerClient.getBlockBlobClient(workItem.targetFile.locator.blobName);
                    const response = await blobClient.beginCopyFromURL(workItem.sourceUrl);
                    await response.pollUntilDone();
                }
            } catch (error) {
                reject(error);
            }
            resolve();
        });

        this.activeWorkItems.push({
            workItem,
            promise,
        });
    }

    finishWorkItemSingle(activeWorkItem: ActiveWorkItem) {
        this.logger.info(`FileCopier.finishWorkItemSingle() - ${activeWorkItem.workItem.sourceFile.locator.url}`);
        if (activeWorkItem.error) {
            this.logger.error(activeWorkItem.error);
            if (activeWorkItem.workItem.retries++ < 1) {
                this.queuedWorkItems.push(activeWorkItem.workItem);
            } else {
                throw activeWorkItem.error;
            }
        } else {
            this.filesCopied++;
            this.bytesCopied += activeWorkItem.workItem.contentLength;
        }
    }

    processWorkItemMultipartStart(workItem: WorkItem) {
        this.logger.info(`FileCopier.processWorkItemMultipartStart() - ${workItem.sourceFile.locator.url}`);

        const promise = new Promise<{ uploadId?: string }>(async (resolve, reject) => {
            let uploadId: string = undefined;

            try {
                if (isS3Locator(workItem.targetFile.locator)) {
                    const s3Client = await this.config.getS3Client(workItem.targetFile.locator.bucket);
                    const commandOutput = await s3Client.send(new CreateMultipartUploadCommand({
                        Bucket: workItem.targetFile.locator.bucket,
                        Key: workItem.targetFile.locator.key,
                        ContentType: workItem.contentType,
                    }));
                    uploadId = commandOutput.UploadId;
                }
            } catch (error) {
                reject(error);
            }
            resolve({ uploadId });
        });

        this.activeWorkItems.push({
            workItem,
            promise,
        });
    }

    finishWorkItemMultipartStart(activeWorkItem: ActiveWorkItem) {
        this.logger.info(`FileCopier.finishWorkItemMultipartStart() - ${activeWorkItem.workItem.sourceFile.locator.url}`);
        if (activeWorkItem.error) {
            this.logger.error(activeWorkItem.error);
            if (activeWorkItem.workItem.retries++ < 1) {
                this.queuedWorkItems.push(activeWorkItem.workItem);
            } else {
                throw activeWorkItem.error;
            }
        } else {
            const { uploadId } = activeWorkItem.result;
            const contentLength = activeWorkItem.workItem.contentLength;

            let maxNumberParts: number = undefined;
            let multipartSize = MULTIPART_SIZE;
            if (isS3Locator(activeWorkItem.workItem.targetFile.locator)) {
                maxNumberParts = 10000;
            } else if (isBlobStorageLocator(activeWorkItem.workItem.targetFile.locator)) {
                maxNumberParts = 50000;
            } else {
                throw new McmaException(`Unsupported locator type '${activeWorkItem.workItem.targetFile.locator["@type"]}'`);
            }

            while (multipartSize * maxNumberParts < contentLength) {
                multipartSize *= 2;
            }

            const segments: MultipartSegment[] = [];

            let bytePosition = 0;
            for (let partNumber = 1; bytePosition < contentLength; partNumber++) {
                const start = bytePosition;
                const end = bytePosition + multipartSize - 1 >= contentLength ? contentLength - 1 : bytePosition + multipartSize - 1;
                const length = end - start + 1;

                const segment: MultipartSegment = {
                    partNumber,
                    start,
                    end,
                    length,
                };

                const segmentWorkItem: WorkItem = Object.assign({}, activeWorkItem.workItem, {
                    type: WorkType.MultipartSegment,
                    retries: 0,
                    multipartData: {
                        uploadId,
                        segment,
                    }
                });

                segments.push(segment);

                bytePosition += length;

                this.queuedWorkItems.push(segmentWorkItem);
            }

            const completeWorkItem: WorkItem = Object.assign({}, activeWorkItem.workItem, {
                type: WorkType.MultipartComplete,
                retries: 0,
                contentType: activeWorkItem.workItem.contentType,
                multipartData: {
                    uploadId,
                    segments,
                }
            });

            this.queuedWorkItems.push(completeWorkItem);
        }
    }

    processWorkItemMultipartSegment(workItem: WorkItem) {
        this.logger.info(`FileCopier.processWorkItemMultipartSegment() - ${workItem.sourceFile.locator.url} - ${workItem.multipartData?.segment?.partNumber}`);

        const promise = new Promise<{ etag?: string, blockId?: string }>(async (resolve, reject) => {
            let etag: string = undefined;
            let blockId: string = undefined;

            try {
                if (isS3Locator(workItem.targetFile.locator)) {
                    const s3Client = await this.config.getS3Client(workItem.targetFile.locator.bucket);

                    if (!workItem.sourceUrl && isS3Locator(workItem.sourceFile.locator)) {
                        const commandOutput = await s3Client.send(new UploadPartCopyCommand({
                            Bucket: workItem.targetFile.locator.bucket,
                            Key: workItem.targetFile.locator.key,
                            CopySource: encodeURI(workItem.sourceFile.locator.bucket + "/" + workItem.sourceFile.locator.key),
                            CopySourceRange: "bytes=" + workItem.multipartData.segment.start + "-" + workItem.multipartData.segment.end,
                            UploadId: workItem.multipartData.uploadId,
                            PartNumber: workItem.multipartData.segment.partNumber,
                        }));

                        etag = commandOutput.CopyPartResult.ETag;
                    } else {
                        const headers = Object.assign({}, this.config.axiosConfig?.headers, { Range: `bytes=${workItem.multipartData.segment.start}-${workItem.multipartData.segment.end}` });
                        const response = await axios.get(workItem.sourceUrl, Object.assign({}, this.config.axiosConfig, {
                            responseType: "arraybuffer",
                            headers
                        }));

                        const commandOutput = await s3Client.send(new UploadPartCommand({
                            Bucket: workItem.targetFile.locator.bucket,
                            Key: workItem.targetFile.locator.key,
                            Body: response.data,
                            UploadId: workItem.multipartData.uploadId,
                            PartNumber: workItem.multipartData.segment.partNumber,
                        }));

                        etag = commandOutput.ETag;
                    }
                } else if (isBlobStorageLocator(workItem.targetFile.locator)) {
                    const containerClient = await this.config.getContainerClient(workItem.targetFile.locator.account, workItem.targetFile.locator.container);
                    const blobClient = containerClient.getBlockBlobClient(workItem.targetFile.locator.blobName);

                    blockId = randomBytes(64).toString("base64");

                    await blobClient.stageBlockFromURL(
                        blockId,
                        workItem.sourceUrl,
                        workItem.multipartData.segment.start,
                        workItem.multipartData.segment.length,
                    );
                } else {
                    throw new McmaException(`Unsupported locator type '${workItem.targetFile.locator["@type"]}'`);
                }
            } catch (error) {
                reject(error);
            }
            resolve({ etag, blockId });
        });

        this.activeWorkItems.push({
            workItem,
            promise,
        });
    }

    finishWorkItemMultipartSegment(activeWorkItem: ActiveWorkItem) {
        this.logger.info(`FileCopier.finishWorkItemMultipartSegment() - ${activeWorkItem.workItem.sourceFile.locator.url} - ${activeWorkItem.workItem.multipartData?.segment?.partNumber}`);
        if (activeWorkItem.error) {
            this.logger.error(activeWorkItem.error);
            if (activeWorkItem.workItem.retries++ < 1) {
                this.queuedWorkItems.push(activeWorkItem.workItem);
            } else {
                throw activeWorkItem.error;
            }
        } else {
            if (isS3Locator(activeWorkItem.workItem.targetFile.locator)) {
                activeWorkItem.workItem.multipartData.segment.etag = activeWorkItem.result.etag;
            } else if (isBlobStorageLocator(activeWorkItem.workItem.targetFile.locator)) {
                activeWorkItem.workItem.multipartData.segment.blockId = activeWorkItem.result.blockId;
            } else {
                throw new McmaException(`Unsupported locator type '${activeWorkItem.workItem.targetFile.locator["@type"]}'`);
            }

            this.bytesCopied += activeWorkItem.workItem.multipartData.segment.length;
        }
    }

    processWorkItemMultipartComplete(workItem: WorkItem) {
        this.logger.info(`FileCopier.processWorkItemMultipartComplete() - ${workItem.sourceFile.locator.url}`);

        const promise = new Promise<void>(async (resolve, reject) => {
            try {
                const hasUnfinishedSegment = !!workItem.multipartData.segments.find(s => !s.etag && !s.blockId);

                if (hasUnfinishedSegment) {
                    await Utils.sleep(1000);
                    this.queuedWorkItems.unshift(workItem);
                } else if (isS3Locator(workItem.targetFile.locator)) {
                    const s3Client = await this.config.getS3Client(workItem.targetFile.locator.bucket);

                    await s3Client.send(
                        new CompleteMultipartUploadCommand({
                            Bucket: workItem.targetFile.locator.bucket,
                            Key: workItem.targetFile.locator.key,
                            UploadId: workItem.multipartData.uploadId,
                            MultipartUpload: {
                                Parts: workItem.multipartData.segments.map(segment => {
                                    return {
                                        ETag: segment.etag,
                                        PartNumber: segment.partNumber,
                                    };
                                }).sort((a, b) => a.PartNumber - b.PartNumber)
                            }
                        })
                    );
                } else if (isBlobStorageLocator(workItem.targetFile.locator)) {
                    const containerClient = await this.config.getContainerClient(workItem.targetFile.locator.account, workItem.targetFile.locator.container);
                    const blobClient = containerClient.getBlockBlobClient(workItem.targetFile.locator.blobName);
                    const blockIds = workItem.multipartData.segments.sort((a, b) => a.partNumber - b.partNumber).map(s => s.blockId);
                    await blobClient.commitBlockList(blockIds, { blobHTTPHeaders: { blobContentType: workItem.contentType } });
                } else {
                    throw new McmaException(`Unsupported locator type '${workItem.targetFile.locator["@type"]}'`);
                }
            } catch (error) {
                reject(error);
            }
            resolve();
        });

        this.activeWorkItems.push({
            workItem,
            promise,
        });
    }

    finishWorkItemMultipartComplete(activeWorkItem: ActiveWorkItem) {
        this.logger.info(`FileCopier.finishWorkItemMultipartComplete() - ${activeWorkItem.workItem.sourceFile.locator.url}`);

        if (activeWorkItem.error) {
            this.logger.error(activeWorkItem.error);
            if (activeWorkItem.workItem.retries++ < 1) {
                this.queuedWorkItems.push(activeWorkItem.workItem);
            } else {
                throw activeWorkItem.error;
            }
        } else {
            this.filesCopied += 1;
        }
    }
}


