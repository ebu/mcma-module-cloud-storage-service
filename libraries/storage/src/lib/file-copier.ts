import { AxiosRequestConfig, default as axios } from "axios";
import { randomBytes } from "crypto";
import {
    CompleteMultipartUploadCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    ListObjectsV2CommandInput,
    PutObjectCommand,
    S3Client,
    UploadPartCommand,
    UploadPartCopyCommand,
} from "@aws-sdk/client-s3";
import { BlobSASPermissions, ContainerClient } from "@azure/storage-blob";

import { Logger, McmaException, Utils } from "@mcma/core";
import { buildS3Url, isS3Locator, S3Locator } from "@mcma/aws-s3";
import { BlobStorageLocator, buildBlobStorageUrl, isBlobStorageLocator } from "@mcma/azure-blob-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

import { ActiveWorkItem, DestinationFile, MultipartSegment, SourceFile, SourceMethod, WorkItem, WorkType, WorkTypePriority } from "./model";
import { destroyStreamOnAbort, logError, raceAbort, withRetry } from "./utils";
import { UrlTrie } from "./url-trie";

const MAX_CONCURRENCY = 32;
const MULTIPART_SEGMENT_BATCH_SIZE = 32;

const KB = 1024;
const MB = 1024 * KB;

const MULTIPART_SIZE = 64 * MB;
const MIN_S3_PART_SIZE = 5 * MB;
const MAX_AZURE_BLOCK_SIZE = 4000 * MB;

export interface FileCopierState {
    filesTotal: number;
    filesCopied: number;
    bytesTotal: number;
    bytesCopied: number;
    workItems: WorkItem[];
    trie: UrlTrie;
}

export interface FileCopierConfig {
    maxConcurrency?: number;
    multipartSize?: number;
    multipartSegmentBatchSize?: number;
    getS3Client: (bucket: string, region?: string) => Promise<S3Client>;
    getContainerClient: (account: string, container: string) => Promise<ContainerClient>;
    progressUpdate?: (filesTotal: number, filesCopied: number, bytesTotal: number, bytesCopied: number) => Promise<void>;
    axiosConfig?: AxiosRequestConfig;
    logger?: Logger;
    debug?: boolean;
}

export class FileCopier {
    private readonly multipartSize: number;
    private readonly multipartSegmentBatchSize: number;

    private readonly queuedWorkItems: WorkItem[];
    private readonly activeWorkItems: ActiveWorkItem[];
    private readonly delayedMultipartCompletes: Set<string>;
    private readonly logger?: Logger;

    private destinationUrls: UrlTrie;

    private maxConcurrency: number;
    private filesTotal: number;
    private filesCopied: number;
    private bytesTotal: number;
    private bytesCopied: number;
    private processing = false;
    private running = false;
    private error?: Error;

    constructor(private config: FileCopierConfig) {
        this.queuedWorkItems = [];
        this.activeWorkItems = [];
        this.delayedMultipartCompletes = new Set<string>();
        this.destinationUrls = new UrlTrie();

        this.filesTotal = 0;
        this.filesCopied = 0;
        this.bytesTotal = 0;
        this.bytesCopied = 0;
        this.logger = this.config.logger;
        this.maxConcurrency = this.config.maxConcurrency > 0 && this.config.maxConcurrency <= 64 ? this.config.maxConcurrency : MAX_CONCURRENCY;
        this.multipartSize = this.config.multipartSize >= MIN_S3_PART_SIZE && this.config.multipartSize <= MAX_AZURE_BLOCK_SIZE ? this.config.multipartSize : MULTIPART_SIZE; // min limit AWS and max limit blob storage
        this.multipartSegmentBatchSize = this.config.multipartSegmentBatchSize > 0 && this.config.multipartSegmentBatchSize <= 1000 ? this.config.multipartSegmentBatchSize : MULTIPART_SEGMENT_BATCH_SIZE;
    }

    public async setState(state: FileCopierState) {
        if (this.running || this.processing) {
            throw new McmaException("Unable to set state while running");
        }

        const workItems = structuredClone(state.workItems);

        // multi part segments were serialized separately from their parent multipart work item. We need to 'link' them together again
        const multipartWorkItems = workItems.filter(w => w.type === WorkType.MultipartComplete || w.type === WorkType.MultipartStart);
        const multipartWorkItemsMap = new Map<string, WorkItem>();
        for (const workItem of multipartWorkItems) {
            if (multipartWorkItemsMap.has(workItem.destinationFile.locator.url)) {
                throw new McmaException(`Invalid state provided. Multiple work items with same destination detected: ${workItem.destinationFile.locator.url}`);
            }
            if (!workItem.multipartData || !Array.isArray(workItem.multipartData.segments)) {
                throw new McmaException(`Invalid state provided. Multipart start/complete work item without valid multipartData property detected: ${workItem.destinationFile.locator.url}`);
            }
            multipartWorkItemsMap.set(workItem.destinationFile.locator.url, workItem);
        }

        for (const workItem of workItems) {
            if (workItem.type === WorkType.MultipartSegment) {
                if (!workItem.multipartData || !workItem.multipartData.segment) {
                    throw new McmaException(`Invalid state provided. Multipart segment work item without valid multipartData property detected: ${workItem.destinationFile.locator.url}`);
                }
                const multipartWorkItem = multipartWorkItemsMap.get(workItem.destinationFile.locator.url);
                if (!multipartWorkItem) {
                    throw new McmaException(`Invalid state provided. Parent multipart work item not found for segment work item: ${workItem.destinationFile.locator.url}`);
                }
                const idx = multipartWorkItem.multipartData.segments.findIndex(s => s.partNumber === workItem.multipartData.segment.partNumber);
                if (idx < 0) {
                    throw new McmaException(`Invalid state provided. Segment not found in parent work item: ${workItem.destinationFile.locator.url} - ${workItem.multipartData.segment.partNumber}`);
                }

                multipartWorkItem.multipartData.segments[idx] = workItem.multipartData.segment;
            }
        }

        this.bytesTotal = state.bytesTotal;
        this.bytesCopied = state.bytesCopied;
        this.filesTotal = state.filesTotal;
        this.filesCopied = state.filesCopied;
        this.queuedWorkItems.length = 0;
        this.activeWorkItems.length = 0;
        this.delayedMultipartCompletes.clear();
        if (!state.trie) {
            throw new McmaException("Invalid state provided. Trie missing");
        }
        this.destinationUrls = await state.trie.clone();

        for (const workItem of workItems) {
            workItem.retries = 0;
            this.queueWorkItem(workItem);
        }
    }

    public async getState(): Promise<FileCopierState> {
        if (this.running || this.processing) {
            throw new McmaException("Unable to get state while running");
        }

        return {
            bytesTotal: this.bytesTotal,
            bytesCopied: this.bytesCopied,
            filesTotal: this.filesTotal,
            filesCopied: this.filesCopied,
            workItems: structuredClone(this.queuedWorkItems),
            trie: await this.destinationUrls.clone(),
        };
    }

    public addFolder(sourceFolder: SourceFile, destinationFolder: DestinationFile) {
        this.queueWorkItem({
            type: WorkType.ScanFolder,
            sourceFile: sourceFolder,
            destinationFile: destinationFolder,
            retries: 0,
        });
    }

    public addFile(sourceFile: SourceFile, destinationFile: DestinationFile) {
        this.queueWorkItem({
            type: WorkType.ScanFile,
            sourceFile,
            destinationFile,
            retries: 0,
        });
    }

    public async runUntil(runUntilDate: Date, bailOutDate: Date) {
        if (runUntilDate >= bailOutDate) {
            throw new McmaException("bailOutDate must be later than runUntilDate");
        }

        this.logger?.debug("FileCopier:runUntil() - Start");
        if (this.running || this.processing) {
            throw new McmaException("Can't invoke method FileCopier:runUntil if it's already invoked");
        }

        this.running = true;
        try {
            this.maxConcurrency = this.config.maxConcurrency > 0 && this.config.maxConcurrency <= 64 ? this.config.maxConcurrency : MAX_CONCURRENCY;

            this.logger?.debug("FileCopier:runUntil() - Starting process thread");

            void this.process();

            this.logger?.debug("FileCopier:runUntil() - Wait until timeout, finished work, or an error");

            while (runUntilDate > new Date() && (this.activeWorkItems.length > 0 || this.queuedWorkItems.length > 0 || this.delayedMultipartCompletes.size > 0) && !this.error) {
                await Utils.sleep(1000);
                if (this.config.progressUpdate) {
                    await this.config.progressUpdate(this.filesTotal, this.filesCopied, this.bytesTotal, this.bytesCopied);
                }
            }

            if (!(runUntilDate > new Date())) {
                this.logger?.debug("FileCopier:runUntil() - Timeout reached");
            } else if (this.error) {
                this.logger?.debug("FileCopier:runUntil() - Error occurred");
            } else {
                this.logger?.debug("FileCopier:runUntil() - Finished work");
            }

            this.maxConcurrency = 0;

            const tenSecondsBeforeBailout = new Date(bailOutDate.getTime() - 10000);

            if (this.processing && (this.activeWorkItems.length > 0 || this.delayedMultipartCompletes.size > 0)) {
                this.logger?.debug("FileCopier:runUntil() - Wait for active work items to finish");

                let aborted = false;

                while (this.activeWorkItems.length > 0 || this.delayedMultipartCompletes.size > 0) {
                    const now = new Date();

                    if (bailOutDate < now) {
                        throw new McmaException("FileCopier:runUntil() - Not able to finish workItems in time. Bailing out");
                    }

                    if (!aborted && tenSecondsBeforeBailout < now) {
                        this.logger?.debug("FileCopier:runUntil() - Reaching 10 seconds before bailout time. Aborting active work items.");
                        // Abort all active workItems 10 seconds before bail out time
                        for (const activeWorkItem of this.activeWorkItems) {
                            if (!activeWorkItem.abortController.signal.aborted) {
                                activeWorkItem.abortController.abort();
                            }
                        }
                        aborted = true;
                    }

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
            this.logger?.debug("FileCopier:runUntil() - Wait for process thread to stop");

            while (this.processing) {
                if (bailOutDate < new Date()) {
                    throw new McmaException("FileCopier:runUntil() - Not able to finish workItems in time. Bailing out");
                }
                await Utils.sleep(250);
            }
        }

        this.logger?.debug("FileCopier:runUntil() - End");
    }

    public getError() {
        return this.error;
    }

    private buildWorkItemLogMessage(workItem: WorkItem, message: string) {
        switch (workItem.type) {
            case WorkType.ScanFolder:
            case WorkType.ScanFile:
            case WorkType.ProcessFolder:
            case WorkType.ProcessFile:
            case WorkType.Single:
            case WorkType.MultipartStart:
            case WorkType.MultipartComplete:
                return `FileCopier:${workItem.type} - ${workItem.destinationFile.locator.url} - ${message}`;
            case WorkType.MultipartSegment:
                return `FileCopier:${workItem.type} - ${workItem.destinationFile.locator.url} - ${workItem.multipartData?.segment?.partNumber} - ${message}`;
        }
    }

    private logDebug(workItem: WorkItem, message: string) {
        if (!!this.config.debug) {
            this.logger?.debug(this.buildWorkItemLogMessage(workItem, message));
        }
    }

    private logInfo(workItem: WorkItem, message: string) {
        this.logger?.info(this.buildWorkItemLogMessage(workItem, message));
    }

    private queueWorkItem(workItem: WorkItem) {
        const index = upperBoundByPriority(this.queuedWorkItems, workItem);
        this.queuedWorkItems.splice(index, 0, workItem);
    }

    private async process() {
        this.logger?.debug("FileCopier:process() - Begin");
        this.processing = true;
        try {
            while (this.running && (this.queuedWorkItems.length > 0 || this.activeWorkItems.length > 0 || this.delayedMultipartCompletes.size > 0)) {
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

                    const err = activeWorkItem.error as any;
                    const code = err?.code;
                    const message = err?.message ?? String(err);

                    if (activeWorkItem.abortController.signal.aborted) {
                        this.logInfo(activeWorkItem.workItem, "Operation aborted. Re-queuing");
                        this.queueWorkItem(activeWorkItem.workItem);
                    } else if (err && code !== "PendingCopyOperation") {
                        this.logInfo(activeWorkItem.workItem, "Error occurred: " + message);
                        logError(this.logger, err);
                        if (activeWorkItem.workItem.retries++ < 2) {
                            this.queueWorkItem(activeWorkItem.workItem);
                        } else {
                            throw err;
                        }
                    } else {
                        const { workItem, result } = activeWorkItem;

                        this.logDebug(workItem, "finish");

                        switch (workItem.type) {
                            case WorkType.ScanFolder:
                                this.finishWorkItemScanFolder(workItem, result);
                                break;
                            case WorkType.ScanFile:
                                this.finishWorkItemScanFile(workItem, result);
                                break;
                            case WorkType.ProcessFolder:
                                this.finishWorkItemProcessFolder(workItem, result);
                                break;
                            case WorkType.ProcessFile:
                                this.finishWorkItemProcessFile(workItem, result);
                                break;
                            case WorkType.Single:
                                this.finishWorkItemSingle(workItem);
                                break;
                            case WorkType.MultipartStart:
                                this.finishWorkItemMultipartStart(workItem, result);
                                break;
                            case WorkType.MultipartSegment:
                                this.finishWorkItemMultipartSegment(workItem, result);
                                break;
                            case WorkType.MultipartComplete:
                                this.finishWorkItemMultipartComplete(workItem, result);
                                break;
                            default:
                                throw new McmaException("Unexpected workItem type " + workItem.type);
                        }
                    }
                }

                // if we have queued WorkItems and we have have not yet reached max concurrency we'll process next work item.
                if (this.queuedWorkItems.length > 0 && this.activeWorkItems.length < this.maxConcurrency) {
                    const workItem = this.queuedWorkItems.shift();

                    this.logDebug(workItem, "process");

                    const abortController = new AbortController();
                    let promise: Promise<any>;
                    switch (workItem.type) {
                        case WorkType.ScanFolder:
                            promise = this.processWorkItemScanFolder(workItem, abortController.signal);
                            break;
                        case WorkType.ScanFile:
                            promise = this.processWorkItemScanFile(workItem, abortController.signal);
                            break;
                        case WorkType.ProcessFolder:
                            promise = this.processWorkItemProcessFolder(workItem, abortController.signal);
                            break;
                        case WorkType.ProcessFile:
                            promise = this.processWorkItemProcessFile(workItem, abortController.signal);
                            break;
                        case WorkType.Single:
                            promise = this.processWorkItemSingle(workItem, abortController.signal);
                            break;
                        case WorkType.MultipartStart:
                            promise = this.processWorkItemMultipartStart(workItem, abortController.signal);
                            break;
                        case WorkType.MultipartSegment:
                            promise = this.processWorkItemMultipartSegment(workItem, abortController.signal);
                            break;
                        case WorkType.MultipartComplete:
                            promise = this.processWorkItemMultipartComplete(workItem, abortController.signal);
                            break;
                        default:
                            throw new McmaException("Unexpected workItem type " + workItem.type);
                    }

                    this.activeWorkItems.push({
                        workItem,
                        promise,
                        abortController,
                    });
                } else {
                    await Utils.sleep(250);
                }
            }
        } catch (error) {
            this.logger?.error("FileCopier:process() - Error caught:");
            logError(this.logger, error);
            this.error = error;
        } finally {
            this.processing = false;
            this.logger?.debug("FileCopier:process() - End");
        }
    }


    /**
     * The goal of start ScanFolder is to scan a page of files, and obtain the count and size of files, compute destination url and add it to collection
     * to test for duplicates
     */
    private async processWorkItemScanFolder(workItem: WorkItem, abortSignal: AbortSignal) {
        const PAGE_SIZE = 1000;

        const sourceFolder = workItem.sourceFile;
        const destinationFolder = workItem.destinationFile;

        let files = 0;
        let bytes = 0;
        let continuationToken: string | undefined;
        let treatAsFile = false;

        let errorMessage: string | undefined;
        let errorContext: {
            sourceFolder: SourceFile
            sourceFile: SourceFile
            destinationFolder: DestinationFile
            destinationFile: DestinationFile
        } | undefined;

        try {
            if (isS3Locator(sourceFolder.locator)) {
                const s3Client = await this.config.getS3Client(sourceFolder.locator.bucket, sourceFolder.locator.region);

                const params: ListObjectsV2CommandInput = {
                    Bucket: sourceFolder.locator.bucket,
                    Prefix: sourceFolder.locator.key,
                    ContinuationToken: workItem.continuationToken,
                    MaxKeys: PAGE_SIZE
                };

                const output = await raceAbort(
                    abortSignal,
                    s3Client.send(new ListObjectsV2Command(params), { abortSignal })
                );
                continuationToken = output.NextContinuationToken;

                if (Array.isArray(output.Contents)) {
                    for (const content of output.Contents) {
                        if (!content.Key) {
                            continue;
                        }

                        files++;
                        bytes += content.Size ?? 0;

                        const sourceFile: SourceFile = {
                            locator: new S3Locator({
                                url: await buildS3Url(sourceFolder.locator.bucket, content.Key, sourceFolder.locator.region)
                            }),
                            egressUrl: sourceFolder.egressUrl ? sourceFolder.egressUrl + content.Key.substring(sourceFolder.locator.key.length) : undefined,
                        };

                        const destinationFile = await buildDestinationFile(sourceFolder, sourceFile, destinationFolder);
                        if (!this.destinationUrls.insert(destinationFile.locator.url)) {
                            errorMessage = `DestinationFile '${destinationFile.locator.url}' already added to FileCopier`;
                            errorContext = {
                                sourceFolder,
                                sourceFile,
                                destinationFolder,
                                destinationFile,
                            };
                            break;
                        }
                    }
                }
            } else if (isBlobStorageLocator(sourceFolder.locator)) {
                const containerClient = await this.config.getContainerClient(sourceFolder.locator.account, sourceFolder.locator.container);

                const iterator = containerClient
                    .listBlobsFlat({
                        prefix: sourceFolder.locator.blobName,
                        abortSignal,
                    })
                    .byPage({
                        maxPageSize: PAGE_SIZE,
                        continuationToken: workItem.continuationToken,
                    });

                const page = await iterator.next();
                if (!page.done && page.value) {
                    const response = page.value;
                    continuationToken = response.continuationToken;

                    for (const blob of response.segment.blobItems) {
                        files++;
                        bytes += blob.properties.contentLength ?? 0;

                        const sourceFile: SourceFile = {
                            locator: new BlobStorageLocator({
                                url: buildBlobStorageUrl(sourceFolder.locator.account, sourceFolder.locator.container, blob.name)
                            }),
                            egressUrl: sourceFolder.egressUrl ? sourceFolder.egressUrl + blob.name.substring(sourceFolder.locator.blobName.length) : undefined,
                        };

                        const destinationFile = await buildDestinationFile(sourceFolder, sourceFile, destinationFolder);
                        if (!this.destinationUrls.insert(destinationFile.locator.url)) {
                            errorMessage = `DestinationFile '${destinationFile.locator.url}' already added to FileCopier`;
                            errorContext = {
                                sourceFolder,
                                sourceFile,
                                destinationFolder,
                                destinationFile,
                            };
                            break;
                        }
                    }
                }
            }
        } catch (error) {
            if (abortSignal.aborted) {
                throw error;
            }

            // in case we get an error we assume it's a permissions and in that case there is nothing we can do
            // In this case we give it one more attempt and try it as a public file.

            logError(this.logger, error);
            treatAsFile = true;
        }

        return {
            files,
            bytes,
            treatAsFile,
            continuationToken,
            errorMessage,
            errorContext,
        };
    }

    /**
     * The goal of finish ScanFolder is to get the output from previous method. Add it to the totals and
     * generate a new ScanFolder workItem if there is a continuationToken or else generate a ProcessFolder workItem
     */
    private finishWorkItemScanFolder(workItem: WorkItem, result: {
        files: number,
        bytes: number,
        treatAsFile: boolean,
        continuationToken?: string,
        errorMessage?: string,
        errorContext?: any,
    }) {
        if (result.errorMessage) {
            throw new McmaException(result.errorMessage, undefined, result.errorContext);
        }

        if (result.treatAsFile) {
            this.queueWorkItem({
                ...workItem,
                type: WorkType.ScanFile,
                retries: 0,
            });
            return;
        }

        this.filesTotal += result.files;
        this.bytesTotal += result.bytes;

        if (result.continuationToken) {
            this.queueWorkItem({
                ...workItem,
                continuationToken: result.continuationToken,
                retries: 0,
            });
        } else {
            this.queueWorkItem({
                ...workItem,
                type: WorkType.ProcessFolder,
                continuationToken: undefined,
                retries: 0,
            });
        }
    }

    /**
     * The goal of start ScanFile is to get the header metadata such as length, type, and lastModified
     */
    private async processWorkItemScanFile(workItem: WorkItem, abortSignal: AbortSignal) {
        let contentLength: number | undefined;
        let contentType: string | undefined;
        let lastModified: Date | undefined;

        if (isS3Locator(workItem.sourceFile.locator)) {
            try {
                const s3Client = await this.config.getS3Client(workItem.sourceFile.locator.bucket, workItem.sourceFile.locator.region);
                const commandOutput = await raceAbort(
                    abortSignal,
                    s3Client.send(new HeadObjectCommand({
                        Bucket: workItem.sourceFile.locator.bucket,
                        Key: workItem.sourceFile.locator.key,
                    }), { abortSignal })
                );

                contentLength = commandOutput.ContentLength;
                contentType = commandOutput.ContentType;
                lastModified = commandOutput.LastModified;
            } catch (error) {
                if (abortSignal.aborted) {
                    throw error;
                }
                const msg = error instanceof Error ? error.message : String(error);
                this.logInfo(workItem, "Ignored error: " + msg);
                logError(this.logger, error);
            }
        } else if (isBlobStorageLocator(workItem.sourceFile.locator)) {
            try {
                const sourceContainerClient = await this.config.getContainerClient(workItem.sourceFile.locator.account, workItem.sourceFile.locator.container);
                const sourceBlobClient = sourceContainerClient.getBlockBlobClient(workItem.sourceFile.locator.blobName);
                const properties = await raceAbort(
                    abortSignal,
                    sourceBlobClient.getProperties({ abortSignal })
                );

                contentLength = properties.contentLength;
                contentType = properties.contentType;
                lastModified = properties.lastModified;
            } catch (error) {
                if (abortSignal.aborted) {
                    throw error;
                }
                const msg = error instanceof Error ? error.message : String(error);
                this.logInfo(workItem, "Ignored error: " + msg);
                logError(this.logger, error);
            }
        }

        if (!Number.isSafeInteger(contentLength) || contentLength < 0) {
            const response = await raceAbort(
                abortSignal,
                axios.head(workItem.sourceFile.egressUrl ?? workItem.sourceFile.locator.url, {
                    ...this.config.axiosConfig,
                    signal: abortSignal,
                })
            );

            contentLength = Number(response.headers["content-length"]);
            contentType = response.headers["content-type"];
            lastModified = new Date(response.headers["last-modified"]);
        }

        if (!Number.isSafeInteger(contentLength) || contentLength < 0) {
            throw new McmaException("Failed to obtain content length");
        }

        return {
            contentLength,
            contentType,
            lastModified: Utils.ensureValidDateOrUndefined(lastModified),
        };
    }

    /**
     * The goal of finish ScanFile is to add to totals and create a ProcessFile work item
     */
    private finishWorkItemScanFile(workItem: WorkItem, result: { contentLength: number, contentType?: string, lastModified?: Date }) {
        if (!this.destinationUrls.insert(workItem.destinationFile.locator.url)) {
            throw new McmaException(`DestinationFile '${workItem.destinationFile.locator.url}' already added to FileCopier`, undefined, {
                sourceFile: workItem.sourceFile,
                destinationFile: workItem.destinationFile,
            });
        }

        if (!Number.isSafeInteger(result.contentLength) || result.contentLength < 0) {
            throw new McmaException("Invalid contentLength for finishWorkItemScanFile work item");
        }

        this.filesTotal++;
        this.bytesTotal += result.contentLength;

        this.queueWorkItem({
            ...workItem,
            type: WorkType.ProcessFile,
            contentLength: result.contentLength,
            contentType: result.contentType,
            lastModified: result.lastModified,
            retries: 0,
        });
    }

    /**
     * The goal of processWorkItemProcessFolder is to generate ProcessFile work items for as long as there are still file
     */
    private async processWorkItemProcessFolder(workItem: WorkItem, abortSignal: AbortSignal) {
        const PAGE_SIZE = 100;

        const sourceFolder = workItem.sourceFile;
        const destinationFolder = workItem.destinationFile;

        let continuationToken: string | undefined;

        const transfers: {
            sourceFile: SourceFile,
            destinationFile: DestinationFile,
            contentLength: number,
            contentType: string,
            lastModified: Date,
        }[] = [];

        if (isS3Locator(sourceFolder.locator)) {
            const s3Client = await this.config.getS3Client(sourceFolder.locator.bucket, sourceFolder.locator.region);

            const params: ListObjectsV2CommandInput = {
                Bucket: sourceFolder.locator.bucket,
                Prefix: sourceFolder.locator.key,
                ContinuationToken: workItem.continuationToken,
                MaxKeys: PAGE_SIZE
            };

            const output = await raceAbort(
                abortSignal,
                s3Client.send(new ListObjectsV2Command(params), { abortSignal })
            );
            continuationToken = output.NextContinuationToken;

            if (Array.isArray(output.Contents)) {
                for (const content of output.Contents) {
                    if (!content.Key) {
                        continue;
                    }

                    const sourceFile: SourceFile = {
                        locator: new S3Locator({
                            url: await buildS3Url(sourceFolder.locator.bucket, content.Key, sourceFolder.locator.region)
                        }),
                        egressUrl: sourceFolder.egressUrl ? sourceFolder.egressUrl + content.Key.substring(sourceFolder.locator.key.length) : undefined,
                    };

                    const destinationFile = await buildDestinationFile(sourceFolder, sourceFile, destinationFolder);

                    const sourceBucket = sourceFolder.locator.bucket;
                    const headOutput = await withRetry(() => s3Client.send(new HeadObjectCommand({
                        Bucket: sourceBucket,
                        Key: content.Key,
                    }), { abortSignal }), abortSignal);


                    if (!Number.isSafeInteger(headOutput.ContentLength) || headOutput.ContentLength < 0) {
                        throw new McmaException("Failed to obtain content length");
                    }

                    transfers.push({
                        sourceFile,
                        destinationFile,
                        contentLength: headOutput.ContentLength,
                        contentType: headOutput.ContentType,
                        lastModified: headOutput.LastModified,
                    });
                }
            }
        } else if (isBlobStorageLocator(sourceFolder.locator)) {
            const containerClient = await this.config.getContainerClient(sourceFolder.locator.account, sourceFolder.locator.container);

            const iterator = containerClient
                .listBlobsFlat({
                    prefix: sourceFolder.locator.blobName,
                    abortSignal,
                })
                .byPage({
                    maxPageSize: PAGE_SIZE,
                    continuationToken: workItem.continuationToken,
                });

            const page = await iterator.next();
            if (!page.done && page.value) {
                const response = page.value;
                continuationToken = response.continuationToken;

                for (const blob of response.segment.blobItems) {
                    const sourceFile: SourceFile = {
                        locator: new BlobStorageLocator({
                            url: buildBlobStorageUrl(sourceFolder.locator.account, sourceFolder.locator.container, blob.name)
                        }),
                        egressUrl: sourceFolder.egressUrl ? sourceFolder.egressUrl + blob.name.substring(sourceFolder.locator.blobName.length) : undefined,
                    };

                    const destinationFile = await buildDestinationFile(sourceFolder, sourceFile, destinationFolder);

                    if (!Number.isSafeInteger(blob.properties.contentLength) || blob.properties.contentLength < 0) {
                        throw new McmaException("Failed to obtain content length");
                    }

                    transfers.push({
                        sourceFile,
                        destinationFile,
                        contentLength: blob.properties.contentLength,
                        contentType: blob.properties.contentType,
                        lastModified: blob.properties.lastModified,
                    });
                }
            }
        }

        return {
            continuationToken,
            transfers,
        };
    }


    /**
     * The goal of finishWorkItemProcessFolder is to queue ProcessFile WorkItems for each of the transfers, and if there is a continuation token another
     * ProcessFolder workItem.
     */
    private finishWorkItemProcessFolder(workItem: WorkItem, result: {
        continuationToken?: string,
        transfers: {
            sourceFile: SourceFile,
            destinationFile: DestinationFile,
            contentLength: number,
            contentType: string,
            lastModified: Date,
        }[],
    }) {

        for (const transfer of result.transfers) {
            this.queueWorkItem({
                type: WorkType.ProcessFile,
                sourceFile: transfer.sourceFile,
                destinationFile: transfer.destinationFile,
                contentLength: transfer.contentLength,
                contentType: transfer.contentType,
                lastModified: transfer.lastModified,
                retries: 0,
            });
        }

        if (result.continuationToken) {
            this.queueWorkItem({
                ...workItem,
                continuationToken: result.continuationToken,
                retries: 0,
            });
        }
    }

    /***
     * The goal of ProcessFile is to figure out how the source should be read (URL or S3 Copy)
     * and to determine whether the target object already exists and should be skipped
     ***/
    private async processWorkItemProcessFile(workItem: WorkItem, abortSignal: AbortSignal) {
        let sourceMethod: SourceMethod | undefined;

        if (workItem.sourceFile.egressUrl) {
            sourceMethod = SourceMethod.EgressUrl;
        } else {
            if (isS3Locator(workItem.sourceFile.locator) && isS3Locator(workItem.destinationFile.locator)) {
                // if both source and target are S3 try to see if we can access the source with target credentials so we can do s3 copy
                const s3Client = await this.config.getS3Client(workItem.destinationFile.locator.bucket, workItem.sourceFile.locator.region);

                try {
                    await raceAbort(
                        abortSignal,
                        s3Client.send(new HeadObjectCommand({
                            Bucket: workItem.sourceFile.locator.bucket,
                            Key: workItem.sourceFile.locator.key,
                        }), { abortSignal })
                    );

                    sourceMethod = SourceMethod.S3Copy;
                } catch (error) {
                    if (abortSignal.aborted) {
                        throw error;
                    }

                    this.logInfo(workItem, "Source AND Target are S3 - FAILED head request");
                    logError(this.logger, error);
                }
            }

            // for all other cases we'll need a URL as a source
            if (!sourceMethod) {
                // first check if locator URL is publicly readable.
                try {
                    await raceAbort(
                        abortSignal,
                        axios.head(workItem.sourceFile.locator.url, {
                            ...this.config.axiosConfig,
                            signal: abortSignal,
                        })
                    );
                    sourceMethod = SourceMethod.LocatorUrl;
                } catch (error) {
                    if (abortSignal.aborted) {
                        throw error;
                    }
                }
            }
        }

        if (!sourceMethod) {
            // if not we'll create a signed URL
            sourceMethod = SourceMethod.SignedUrl;
        }

        // now we check if target already exists and has the same contentLength and a newer lastModified date
        // this means it's most likely an exact copy.
        let targetContentLength: number | undefined;
        let targetContentType: string | undefined;
        let targetLastModified: Date | undefined;

        try {
            if (isS3Locator(workItem.destinationFile.locator)) {
                const s3Client = await this.config.getS3Client(workItem.destinationFile.locator.bucket);

                const headOutput = await raceAbort(
                    abortSignal,
                    s3Client.send(new HeadObjectCommand({
                        Bucket: workItem.destinationFile.locator.bucket,
                        Key: workItem.destinationFile.locator.key
                    }), { abortSignal })
                );

                targetContentLength = headOutput.ContentLength;
                targetContentType = headOutput.ContentType;
                targetLastModified = headOutput.LastModified;
            } else if (isBlobStorageLocator(workItem.destinationFile.locator)) {
                const containerClient = await this.config.getContainerClient(workItem.destinationFile.locator.account, workItem.destinationFile.locator.container);
                const blobClient = containerClient.getBlobClient(workItem.destinationFile.locator.blobName);
                const propertiesResponse = await raceAbort(
                    abortSignal,
                    blobClient.getProperties({ abortSignal })
                );

                targetContentLength = propertiesResponse.contentLength;
                targetContentType = propertiesResponse.contentType;
                targetLastModified = propertiesResponse.lastModified;
            }
        } catch (error: any) {
            if (abortSignal.aborted) {
                throw error;
            }
            const msg = error instanceof Error ? error.message : String(error);

            const status = error?.$metadata?.httpStatusCode ?? error?.statusCode; // fetching status code from AWS and Azure error

            if (status !== 404) {
                this.logInfo(workItem, `Failed reading target metadata: ${msg}`);
                logError(this.logger, error);
            }
        }

        // Normalize content-types for comparison
        const normalizeType = (t?: string) => t ? t.split(";")[0].trim().toLowerCase() : undefined;
        const srcType = normalizeType(workItem.contentType);
        const dstType = normalizeType(targetContentType);

        // Compare lengths safely (only if both are safe integers)
        const srcLenOk = Number.isSafeInteger(workItem.contentLength);
        const dstLenOk = Number.isSafeInteger(targetContentLength);

        const lengthsEqual = srcLenOk && dstLenOk && workItem.contentLength === targetContentLength;
        const typesEqual = srcType === dstType;

        // Compare lastModified: skip only if target is same or newer (allow undefined to be false)
        const targetIsSameOrNewer = (workItem.lastModified && targetLastModified)
            ? targetLastModified.getTime() >= workItem.lastModified.getTime()
            : false;

        const skip = lengthsEqual && typesEqual && targetIsSameOrNewer;

        return { sourceMethod, skip };
    }

    /**
     * This function gets the result of work item process file and creates workItems MultipartStart or Single based on file size
     */
    private finishWorkItemProcessFile(workItem: WorkItem, result: { sourceMethod: SourceMethod, skip: boolean }) {
        if (!Number.isSafeInteger(workItem.contentLength) || workItem.contentLength < 0) {
            throw new McmaException("Invalid contentLength for finishWorkItemProcessFile work item");
        }

        if (result.skip) {
            this.logInfo(workItem, "File already present on target location. Skipping");
            this.filesTotal--;
            this.bytesTotal -= workItem.contentLength;
            return;
        }

        if (workItem.contentLength > this.multipartSize) {
            this.queueWorkItem({
                ...workItem,
                type: WorkType.MultipartStart,
                sourceMethod: result.sourceMethod,
                multipartData: {
                    segments: []
                },
                retries: 0,
            });
        } else {
            this.queueWorkItem({
                ...workItem,
                type: WorkType.Single,
                sourceMethod: result.sourceMethod,
                retries: 0,
            });
        }
    }

    /**
     * Single work item copies a file in one go (no multipart)
     */
    private async processWorkItemSingle(workItem: WorkItem, abortSignal: AbortSignal) {
        if (workItem.sourceMethod === SourceMethod.S3Copy) {
            const sourceLocator = workItem.sourceFile.locator as S3Locator;
            const destinationLocator = workItem.destinationFile.locator as S3Locator;

            const s3Client = await this.config.getS3Client(destinationLocator.bucket, destinationLocator.region);
            await raceAbort(
                abortSignal,
                s3Client.send(new CopyObjectCommand({
                        Bucket: destinationLocator.bucket,
                        Key: destinationLocator.key,
                        CopySource: `${sourceLocator.bucket}/${encodeURIComponent(sourceLocator.key)}`,
                        StorageClass: workItem.destinationFile.storageClass,
                    }), { abortSignal }
                )
            );
        } else {
            let sourceUrl = await this.obtainSourceUrlFromWorkItem(workItem);
            if (!sourceUrl) {
                throw new McmaException(`Failed to resolve source url for WorkItemSingle - ${workItem.destinationFile.locator.url}`);
            }

            if (isS3Locator(workItem.destinationFile.locator)) {
                const s3Client = await this.config.getS3Client(workItem.destinationFile.locator.bucket);

                // 1) Start the GET (abortable) and race it so we don't hang waiting for headers
                const response = await raceAbort(
                    abortSignal,
                    axios.get(sourceUrl, {
                        ...this.config.axiosConfig,
                        responseType: "stream",
                        decompress: false,
                        signal: abortSignal,
                    })
                );
                if (response.status < 200 || response.status >= 300) {
                    const msg = `GET ${sourceUrl} returned ${response.status}`;
                    this.logInfo(workItem, msg);
                    throw new McmaException(msg);
                }
                if (!response.data) {
                    const msg = `GET ${sourceUrl} returned no response.data`;
                    this.logInfo(workItem, msg);
                    throw new McmaException(msg);
                }

                // 2) Ensure the body stream is destroyed on abort
                destroyStreamOnAbort(response.data, abortSignal);

                const contentLengthHeader = response.headers["content-length"];
                if (contentLengthHeader !== undefined) {
                    const n = Number(contentLengthHeader);
                    if (!Number.isFinite(n) || n !== workItem.contentLength) {
                        const msg = `Content-Length mismatch. Expected ${workItem.contentLength}, got ${contentLengthHeader}`;
                        this.logInfo(workItem, msg);
                        throw new McmaException(msg);
                    }
                }

                await raceAbort(
                    abortSignal,
                    s3Client.send(new PutObjectCommand({
                        Bucket: workItem.destinationFile.locator.bucket,
                        Key: workItem.destinationFile.locator.key,
                        Body: response.data,
                        ContentType: workItem.contentType,
                        StorageClass: workItem.destinationFile.storageClass,
                        ContentLength: workItem.contentLength,
                    }), { abortSignal })
                );
            } else if (isBlobStorageLocator(workItem.destinationFile.locator)) {
                const containerClient = await this.config.getContainerClient(workItem.destinationFile.locator.account, workItem.destinationFile.locator.container);
                const blobClient = containerClient.getBlockBlobClient(workItem.destinationFile.locator.blobName);

                const poller = await raceAbort(
                    abortSignal,
                    blobClient.beginCopyFromURL(sourceUrl, { abortSignal })
                );
                while (true) {
                    if (abortSignal.aborted) {
                        throw new McmaException("Azure copy operation aborted");
                    }

                    await raceAbort(
                        abortSignal,
                        poller.poll({ abortSignal })
                    );
                    const state = poller.getOperationState();

                    if (state.isCompleted) {
                        if (state.error) {
                            throw new McmaException(`Azure copy failed: ${state.error.message ?? "Unknown error"}`);
                        }

                        break;
                    }

                    await Utils.sleep(1000);
                }

                const props = await raceAbort(
                    abortSignal,
                    blobClient.getProperties({ abortSignal })
                );
                const copyStatus = props.copyStatus;

                if (copyStatus && copyStatus !== "success") {
                    throw new McmaException(`Azure copy did not succeed. copyStatus=${copyStatus}`);
                }

                if (workItem.contentType) {
                    await raceAbort(
                        abortSignal,
                        blobClient.setHTTPHeaders({ blobContentType: workItem.contentType }, { abortSignal })
                    );
                }
            } else {
                throw new McmaException("Unexpected locator type " + workItem.destinationFile.locator["@type"]);
            }
        }
    }

    /**
     * Finish single work item registers files and bytes copied
     */
    private finishWorkItemSingle(workItem: WorkItem) {
        if (!Number.isSafeInteger(workItem.contentLength) || workItem.contentLength < 0) {
            throw new McmaException("Invalid contentLength for finishWorkItemSingle work item");
        }

        this.filesCopied++;
        this.bytesCopied += workItem.contentLength;
    }

    /**
     * The goal of a MultipartStart work item is to start a multipart upload in case the destination is AWS S3
     */
    private async processWorkItemMultipartStart(workItem: WorkItem, abortSignal: AbortSignal) {
        let s3UploadId: string | undefined;
        let blockIdPrefix: string | undefined;

        if (workItem.multipartData?.s3UploadId) {
            s3UploadId = workItem.multipartData?.s3UploadId;
        } else if (isS3Locator(workItem.destinationFile.locator)) {
            const s3Client = await this.config.getS3Client(workItem.destinationFile.locator.bucket);
            const commandOutput = await raceAbort(
                abortSignal,
                s3Client.send(new CreateMultipartUploadCommand({
                    Bucket: workItem.destinationFile.locator.bucket,
                    Key: workItem.destinationFile.locator.key,
                    ContentType: workItem.contentType,
                    StorageClass: workItem.destinationFile.storageClass,
                }), { abortSignal })
            );
            s3UploadId = commandOutput.UploadId;
            if (!s3UploadId) {
                throw new McmaException(`CreateMultipartUploadCommand returned uploadId: ${s3UploadId}`);
            }
        } else if (isBlobStorageLocator(workItem.destinationFile.locator)) {
            blockIdPrefix = randomBytes(16).toString("base64"); // 16 bytes <= 64 byte limit after composition
        }

        return { s3UploadId, blockIdPrefix };
    }


    /**
     * The goal of finish Multipart Start is to generate the next segments for doing the multipart upload. In case more segments are required, it will
     * create a new multipart start work item with indications for the remainder to be generated. When all segment work items are generated it'll generate
     * a Multipart complete work item.
     */
    private finishWorkItemMultipartStart(workItem: WorkItem, result: { s3UploadId?: string, blockIdPrefix?: string }) {
        if (!Number.isSafeInteger(workItem.contentLength) || workItem.contentLength < 0) {
            throw new McmaException("Invalid contentLength for finishWorkItemSingle work item");
        }

        const s3UploadId = result.s3UploadId;
        const blockIdPrefix = result.blockIdPrefix;
        const contentLength = workItem.contentLength;

        let multipartSize = workItem.multipartData?.multipartSize;
        if (!multipartSize) {
            multipartSize = this.multipartSize;
            let maxNumberParts: number;
            if (isS3Locator(workItem.destinationFile.locator)) {
                maxNumberParts = 10000;
            } else if (isBlobStorageLocator(workItem.destinationFile.locator)) {
                maxNumberParts = 50000;
            } else {
                throw new McmaException(`Unsupported locator type '${workItem.destinationFile.locator["@type"]}'`);
            }

            while (multipartSize * maxNumberParts < contentLength) {
                multipartSize *= 2;
            }
        }

        const newSegments: MultipartSegment[] = [];

        let bytePosition = workItem.multipartData?.nextBytePosition ?? 0;
        let partNumber = workItem.multipartData?.nextPartNumber ?? 1;
        while (bytePosition < contentLength) {
            const start = bytePosition;
            const end = bytePosition + multipartSize - 1 >= contentLength ? contentLength - 1 : bytePosition + multipartSize - 1;
            const length = end - start + 1;

            const segment: MultipartSegment = {
                partNumber,
                start,
                end,
                length,
            };

            const segmentWorkItem: WorkItem = {
                ...workItem,
                type: WorkType.MultipartSegment,
                multipartData: {
                    s3UploadId,
                    blockIdPrefix,
                    segment,
                },
                retries: 0,
            };

            this.queueWorkItem(segmentWorkItem);

            bytePosition += length;
            partNumber++;

            newSegments.push(segment);
            if (newSegments.length >= this.multipartSegmentBatchSize) {
                break;
            }
        }

        const prior = workItem.multipartData?.segments ?? [];
        const segments = [...prior, ...newSegments];

        let multipartWorkItem: WorkItem;

        // Still need to generate more segment
        if (bytePosition < contentLength) {
            multipartWorkItem = {
                ...workItem,
                multipartData: {
                    nextPartNumber: partNumber,
                    nextBytePosition: bytePosition,
                    multipartSize,
                    s3UploadId,
                    blockIdPrefix,
                    segments,
                },
                retries: 0,
            };
        } else {
            // Done generating segments
            multipartWorkItem = {
                ...workItem,
                type: WorkType.MultipartComplete,
                multipartData: {
                    s3UploadId,
                    blockIdPrefix,
                    segments,
                },
                retries: 0,
            };
        }

        this.queueWorkItem(multipartWorkItem);
    }

    private async obtainSourceUrlFromWorkItem(workItem: WorkItem) {
        let sourceUrl: string;

        switch (workItem.sourceMethod) {
            case SourceMethod.EgressUrl:
                sourceUrl = workItem.sourceFile.egressUrl;
                break;
            case SourceMethod.LocatorUrl:
                sourceUrl = workItem.sourceFile.locator.url;
                break;
            case SourceMethod.SignedUrl:
                if (isS3Locator(workItem.sourceFile.locator)) {
                    const sourceS3Client = await this.config.getS3Client(workItem.sourceFile.locator.bucket, workItem.sourceFile.locator.region);
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
                } else {
                    throw new McmaException("Unexpected locator type " + workItem.sourceFile.locator["@type"]);
                }
                break;
            default:
                throw new McmaException("Unexpected source method " + workItem.sourceMethod);
        }
        return sourceUrl;
    }

    private async processWorkItemMultipartSegment(workItem: WorkItem, abortSignal: AbortSignal) {
        let etag: string | undefined;
        let blockId: string | undefined;

        if (workItem.sourceMethod === SourceMethod.S3Copy) {
            const sourceLocator = workItem.sourceFile.locator as S3Locator;
            const destinationLocator = workItem.destinationFile.locator as S3Locator;

            const s3Client = await this.config.getS3Client(destinationLocator.bucket, destinationLocator.region);

            const commandOutput = await raceAbort(
                abortSignal,
                s3Client.send(new UploadPartCopyCommand({
                    Bucket: destinationLocator.bucket,
                    Key: destinationLocator.key,
                    CopySource: `${sourceLocator.bucket}/${encodeURIComponent(sourceLocator.key)}`,
                    CopySourceRange: `bytes=${workItem.multipartData.segment.start}-${workItem.multipartData.segment.end}`,
                    UploadId: workItem.multipartData.s3UploadId,
                    PartNumber: workItem.multipartData.segment.partNumber,
                }), { abortSignal })
            );

            const result = commandOutput.CopyPartResult;
            if (!result?.ETag) {
                throw new McmaException("UploadPartCopyCommand did not return ETag");
            }

            etag = result.ETag;
        } else {
            let sourceUrl = await this.obtainSourceUrlFromWorkItem(workItem);
            if (!sourceUrl) {
                throw new McmaException("Failed to resolve source url for MultipartSegment");
            }

            if (isS3Locator(workItem.destinationFile.locator)) {
                const s3Client = await this.config.getS3Client(workItem.destinationFile.locator.bucket);

                const start = workItem.multipartData.segment.start;
                const end = workItem.multipartData.segment.end;

                // 1) Start the GET (abortable) and race it so we don't hang waiting for headers
                const response = await raceAbort(
                    abortSignal,
                    axios.get(sourceUrl, {
                        ...this.config.axiosConfig,
                        responseType: "stream",
                        decompress: false,
                        headers: {
                            ...(this.config.axiosConfig?.headers ?? {}),
                            Range: `bytes=${start}-${end}`,
                        },
                        signal: abortSignal,
                    })
                );

                if (response.status !== 206) {
                    const msg = `Expected 206 Partial Content but received ${response.status}`;
                    this.logInfo(workItem, msg);
                    throw new McmaException(msg);
                }

                const contentRange = response.headers["content-range"];
                if (!contentRange) {
                    const msg = "Missing Content-Range header in partial response";
                    this.logInfo(workItem, msg);
                    throw new McmaException(msg);
                }

                // Example Content-Range: "bytes 0-1048575/8388608"
                const contentRangeMatch = /^bytes (\d+)-(\d+)\/(\d+|\*)$/.exec(contentRange);
                if (!contentRangeMatch) {
                    const msg = `Unexpected Content-Range '${contentRange}'`;
                    this.logInfo(workItem, msg);
                    throw new McmaException(msg);
                }

                const rangeStart = Number(contentRangeMatch[1]);
                const rangeEnd = Number(contentRangeMatch[2]);
                if (!Number.isFinite(rangeStart) || !Number.isFinite(rangeEnd) || rangeStart !== start || rangeEnd !== end) {
                    const msg = `Unexpected Content-Range '${contentRange}', expected start ${start} and end <= ${end}`;
                    this.logInfo(workItem, msg);
                    throw new McmaException(msg);
                }

                const contentLengthHeader = response.headers["content-length"];
                if (contentLengthHeader !== undefined) {
                    const n = Number(contentLengthHeader);
                    if (!Number.isFinite(n) || n !== workItem.multipartData.segment.length) {
                        const msg = `Content-Length mismatch. Expected ${workItem.multipartData.segment.length}, got ${contentLengthHeader}`;
                        this.logInfo(workItem, msg);
                        throw new McmaException(msg);
                    }
                }

                if (!response.data) {
                    const msg = `GET ${sourceUrl} returned no response.data`;
                    this.logInfo(workItem, msg);
                    throw new McmaException(msg);
                }

                // 2) Ensure the body stream is destroyed on abort
                destroyStreamOnAbort(response.data, abortSignal);

                // 3) Start the S3 upload (also abortable) and race it so our promise settles on abort
                const commandOutput = await raceAbort(
                    abortSignal,
                    s3Client.send(new UploadPartCommand({
                        Bucket: workItem.destinationFile.locator.bucket,
                        Key: workItem.destinationFile.locator.key,
                        Body: response.data,
                        UploadId: workItem.multipartData.s3UploadId,
                        PartNumber: workItem.multipartData.segment.partNumber,
                        ContentLength: workItem.multipartData.segment.length,
                    }), { abortSignal })
                );

                const etagValue = commandOutput.ETag;
                if (!etagValue) {
                    throw new McmaException("UploadPartCommand did not return ETag");
                }

                etag = etagValue;
            } else if (isBlobStorageLocator(workItem.destinationFile.locator)) {
                const containerClient = await this.config.getContainerClient(workItem.destinationFile.locator.account, workItem.destinationFile.locator.container);
                const blobClient = containerClient.getBlockBlobClient(workItem.destinationFile.locator.blobName);

                const prefix = workItem.multipartData?.blockIdPrefix;
                if (!prefix) {
                    throw new McmaException("Missing blockIdPrefix for Azure multipart segment");
                }

                const prefixBytes = Buffer.from(prefix, "base64");

                const partNumber = workItem.multipartData.segment.partNumber;
                const partBytes = Buffer.alloc(4);
                partBytes.writeUInt32BE(partNumber);

                blockId = Buffer.concat([prefixBytes, partBytes]).toString("base64");

                await raceAbort(
                    abortSignal,
                    blobClient.stageBlockFromURL(
                        blockId,
                        sourceUrl,
                        workItem.multipartData.segment.start,
                        workItem.multipartData.segment.length,
                        { abortSignal }
                    )
                );
            } else {
                throw new McmaException("Unexpected locator type " + workItem.destinationFile.locator["@type"]);
            }
        }

        return { etag, blockId };
    }

    private finishWorkItemMultipartSegment(workItem: WorkItem, result: { etag: string, blockId: string }) {
        if (isS3Locator(workItem.destinationFile.locator)) {
            workItem.multipartData.segment.etag = result.etag;
        } else if (isBlobStorageLocator(workItem.destinationFile.locator)) {
            workItem.multipartData.segment.blockId = result.blockId;
        } else {
            throw new McmaException(`Unsupported locator type '${workItem.destinationFile.locator["@type"]}'`);
        }

        this.bytesCopied += workItem.multipartData.segment.length;
    }

    private async processWorkItemMultipartComplete(workItem: WorkItem, abortSignal: AbortSignal) {
        let completed: boolean;

        if (isS3Locator(workItem.destinationFile.locator)) {
            completed = !workItem.multipartData.segments.some(s => !s.etag);
            if (completed) {
                const s3Client = await this.config.getS3Client(workItem.destinationFile.locator.bucket);

                await raceAbort(
                    abortSignal,
                    s3Client.send(new CompleteMultipartUploadCommand({
                        Bucket: workItem.destinationFile.locator.bucket,
                        Key: workItem.destinationFile.locator.key,
                        UploadId: workItem.multipartData.s3UploadId,
                        MultipartUpload: {
                            Parts: workItem.multipartData.segments.map(segment => {
                                return {
                                    ETag: segment.etag,
                                    PartNumber: segment.partNumber,
                                };
                            }).sort((a, b) => a.PartNumber - b.PartNumber)
                        }
                    }), { abortSignal })
                );
            }
        } else if (isBlobStorageLocator(workItem.destinationFile.locator)) {
            completed = !workItem.multipartData.segments.some(s => !s.blockId);
            if (completed) {
                const containerClient = await this.config.getContainerClient(workItem.destinationFile.locator.account, workItem.destinationFile.locator.container);
                const blobClient = containerClient.getBlockBlobClient(workItem.destinationFile.locator.blobName);
                const blockIds = [...workItem.multipartData.segments].sort((a, b) => a.partNumber - b.partNumber).map(s => s.blockId);
                await raceAbort(
                    abortSignal,
                    blobClient.commitBlockList(blockIds, { blobHTTPHeaders: { blobContentType: workItem.contentType }, abortSignal })
                );
            }
        } else {
            throw new McmaException(`Unsupported locator type '${workItem.destinationFile.locator["@type"]}'`);
        }

        return { completed };
    }

    private finishWorkItemMultipartComplete(workItem: WorkItem, result: { completed: boolean }) {
        if (result.completed) {
            this.filesCopied++;
        } else {
            const key = workItem.destinationFile.locator.url;

            if (!this.delayedMultipartCompletes.has(key)) {
                this.delayedMultipartCompletes.add(key);

                setTimeout(() => {
                    this.delayedMultipartCompletes.delete(key);
                    this.queueWorkItem(workItem);
                }, 1000);
            }
        }
    }
}

async function buildDestinationFile(sourceFolder: SourceFile, sourceFile: SourceFile, destinationFolder: DestinationFile): Promise<DestinationFile> {
    if (isS3Locator(sourceFolder.locator) && isS3Locator(sourceFile.locator)) {
        if (isS3Locator(destinationFolder.locator)) {
            return {
                locator: new S3Locator({ url: await buildS3Url(destinationFolder.locator.bucket, destinationFolder.locator.key + sourceFile.locator.key.substring(sourceFolder.locator.key.length), destinationFolder.locator.region) }),
                storageClass: destinationFolder.storageClass,
            };
        } else if (isBlobStorageLocator(destinationFolder.locator)) {
            return {
                locator: new BlobStorageLocator({ url: buildBlobStorageUrl(destinationFolder.locator.account, destinationFolder.locator.container, destinationFolder.locator.blobName + sourceFile.locator.key.substring(sourceFolder.locator.key.length)) })
            };
        }
    } else if (isBlobStorageLocator(sourceFolder.locator) && isBlobStorageLocator(sourceFile.locator)) {
        if (isS3Locator(destinationFolder.locator)) {
            return {
                locator: new S3Locator({ url: await buildS3Url(destinationFolder.locator.bucket, destinationFolder.locator.key + sourceFile.locator.blobName.substring(sourceFolder.locator.blobName.length), destinationFolder.locator.region) }),
                storageClass: destinationFolder.storageClass,
            };
        } else if (isBlobStorageLocator(destinationFolder.locator)) {
            return {
                locator: new BlobStorageLocator({ url: buildBlobStorageUrl(destinationFolder.locator.account, destinationFolder.locator.container, destinationFolder.locator.blobName + sourceFile.locator.blobName.substring(sourceFolder.locator.blobName.length)) })
            };
        }
    }

    throw new McmaException("FileCopier:buildDestinationFile - invalid input");
}

function getPriority(t: WorkType) {
    return WorkTypePriority[t] ?? Number.MAX_SAFE_INTEGER;
}

function upperBoundByPriority(array: WorkItem[], value: WorkItem): number {
    const p = getPriority(value.type);
    let low = 0, high = array.length;
    while (low < high) {
        const mid = (low + high) >>> 1;
        if (getPriority(array[mid].type) <= p) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}
