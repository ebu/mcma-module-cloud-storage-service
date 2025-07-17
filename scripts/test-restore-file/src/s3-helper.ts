import * as fs from "fs";
import * as mime from "mime-types";
import {
    AbortMultipartUploadCommand,
    CompleteMultipartUploadCommand,
    CreateMultipartUploadCommand,
    HeadObjectCommand,
    S3Client,
    UploadPartCommand, UploadPartCommandInput, UploadPartCommandOutput,
    UploadPartCopyCommand, UploadPartCopyCommandInput, UploadPartCopyCommandOutput,
    CompletedPart, CopyObjectCommand, DeleteObjectCommand,
    StorageClass
} from "@aws-sdk/client-s3";

interface MultiPartUploadRequest {
    input: UploadPartCommandInput;
    promise?: Promise<UploadPartCommandOutput>;
    output?: UploadPartCommandOutput;
    error?: any;
}

interface MultiPartCopyRequest {
    input: UploadPartCopyCommandInput;
    promise?: Promise<UploadPartCopyCommandOutput>;
    output?: UploadPartCopyCommandOutput;
    error?: any;
}

export interface S3HelperConfig {
    maxConcurrentTransfers?: number;
    multipartSize?: number;
    s3ClientProvider?: (bucket: string) => Promise<S3Client>;
}

async function processRequest(uploadingRequests: MultiPartUploadRequest[], finishedRequests: MultiPartUploadRequest[], errorRequests: MultiPartUploadRequest[]) {
    const request = await Promise.race(uploadingRequests.map(request =>
        request.promise.then(output => {
            request.output = output;
            return request;
        }).catch(error => {
            request.error = error;
            return request;
        })
    ));

    if (request.output) {
        finishedRequests.push(request);
    } else {
        errorRequests.push(request);
    }

    let idx = uploadingRequests.indexOf(request);
    uploadingRequests.splice(idx, 1);
}

export class S3Helper {
    private readonly maxConcurrentTransfers: number;
    private readonly multipartSize: number;
    private readonly s3ClientProvider: (bucket: string) => Promise<S3Client>;
    private readonly bucketS3ClientMap: { [bucket: string]: S3Client } = {};

    constructor(config?: S3HelperConfig) {
        this.maxConcurrentTransfers = config?.maxConcurrentTransfers;
        this.multipartSize = config?.multipartSize;
        this.s3ClientProvider = config?.s3ClientProvider;

        if (typeof this.maxConcurrentTransfers !== "number" || this.maxConcurrentTransfers < 1) {
            this.maxConcurrentTransfers = 64;
        }

        if ((typeof this.multipartSize !== "number") || this.multipartSize < 5242880 || this.multipartSize > 5368709120) {
            this.multipartSize = 128 * 1024 * 1024;
        }
    }

    async getS3Client(bucket: string): Promise<S3Client> {
        if (!this.bucketS3ClientMap[bucket]) {
            this.bucketS3ClientMap[bucket] = await this.s3ClientProvider(bucket);
        }
        return this.bucketS3ClientMap[bucket];
    }

    async upload(filename: string, targetBucket: string, targetKey: string, storageClass?: StorageClass) {
        const s3Client = await this.getS3Client(targetBucket);

        const createResponse = await s3Client.send(new CreateMultipartUploadCommand({
            Bucket: targetBucket,
            Key: targetKey,
            ContentType: mime.lookup(targetKey) || "application/octet-stream",
            StorageClass: storageClass
        }));

        const uploadId = createResponse.UploadId;

        try {
            const stats = fs.statSync(filename);

            const objectSize = stats.size;
            const preparedRequests: MultiPartUploadRequest[] = [];
            const uploadingRequests: MultiPartUploadRequest[] = [];
            const finishedRequests: MultiPartUploadRequest[] = [];
            const errorRequests: MultiPartUploadRequest[] = [];

            let bytePosition = 0;
            for (let i = 1; bytePosition < objectSize; i++) {
                const start = bytePosition;
                const end = (bytePosition + this.multipartSize - 1 >= objectSize ? objectSize - 1 : bytePosition + this.multipartSize - 1);
                const length = end - start + 1;

                preparedRequests.push({
                    input: {
                        Bucket: targetBucket,
                        Key: targetKey,
                        Body: fs.createReadStream(filename, {
                            start,
                            end,
                        }),
                        ContentLength: length,
                        PartNumber: i,
                        UploadId: uploadId,
                    }
                });

                bytePosition += this.multipartSize;
            }

            while (preparedRequests.length > 0) {
                if (uploadingRequests.length >= this.maxConcurrentTransfers) {
                    await processRequest(uploadingRequests, finishedRequests, errorRequests);
                }

                const request = preparedRequests.shift();
                request.promise = s3Client.send(new UploadPartCommand(request.input));
                uploadingRequests.push(request);
            }

            while (uploadingRequests.length > 0) {
                await processRequest(uploadingRequests, finishedRequests, errorRequests);
            }

            if (errorRequests.length > 0) {
                throw new Error("Transfer failed with " + errorRequests.length + " errors");
            }

            const parts: CompletedPart[] = finishedRequests.map(request => {
                return {
                    ETag: request.output.ETag,
                    PartNumber: request.input.PartNumber,
                };
            });
            parts.sort((a, b) => a.PartNumber - b.PartNumber);

            return s3Client.send(new CompleteMultipartUploadCommand({
                Bucket: targetBucket,
                Key: targetKey,
                UploadId: uploadId,
                MultipartUpload: {
                    Parts: parts,
                }
            }));
        } catch (error) {
            await s3Client.send(new AbortMultipartUploadCommand({
                Bucket: targetBucket,
                Key: targetKey,
                UploadId: uploadId,
            }));
            throw error;
        }
    }

    async copy(sourceBucket: string, sourceKey: string, targetBucket: string, targetKey: string) {
        const metadata = await this.head(sourceBucket, sourceKey);
        const objectSize = metadata.ContentLength;

        const s3Client = await this.getS3Client(targetBucket);

        if (objectSize < 2 * this.multipartSize) {
            return s3Client.send(new CopyObjectCommand({
                Bucket: targetBucket,
                Key: targetKey,
                CopySource: `/${sourceBucket}/${sourceKey}`,
                ContentType: mime.lookup(targetKey) || "application/octet-stream"
            }));
        } else {
            const createResponse = await s3Client.send(new CreateMultipartUploadCommand({
                Bucket: targetBucket,
                Key: targetKey,
                ContentType: mime.lookup(targetKey) || "application/octet-stream"
            }));

            const uploadId = createResponse.UploadId;

            try {
                const preparedRequests: MultiPartCopyRequest[] = [];
                const uploadingRequests: MultiPartCopyRequest[] = [];
                const finishedRequests: MultiPartCopyRequest[] = [];
                const errorRequests: MultiPartCopyRequest[] = [];

                let bytePosition = 0;
                for (let i = 1; bytePosition < objectSize; i++) {
                    const start = bytePosition;
                    const end = (bytePosition + this.multipartSize - 1 >= objectSize ? objectSize - 1 : bytePosition + this.multipartSize - 1);

                    preparedRequests.push({
                        input: {
                            Bucket: targetBucket,
                            Key: targetKey,
                            CopySource: encodeURI(sourceBucket + "/" + sourceKey),
                            CopySourceRange: "bytes=" + start + "-" + end,
                            PartNumber: i,
                            UploadId: uploadId,
                        }
                    });

                    bytePosition += this.multipartSize;
                }

                while (preparedRequests.length > 0) {
                    if (uploadingRequests.length >= this.maxConcurrentTransfers) {
                        await processRequest(uploadingRequests, finishedRequests, errorRequests);
                    }

                    const request = preparedRequests.shift();
                    request.promise = s3Client.send(new UploadPartCopyCommand(request.input));
                    uploadingRequests.push(request);
                }

                while (uploadingRequests.length > 0) {
                    await processRequest(uploadingRequests, finishedRequests, errorRequests);
                }

                if (errorRequests.length > 0) {
                    throw new Error("Transfer failed with " + errorRequests.length + " errors");
                }

                const parts: CompletedPart[] = finishedRequests.map(request => {
                    return {
                        ETag: request.output.CopyPartResult.ETag,
                        PartNumber: request.input.PartNumber,
                    };
                });
                parts.sort((a, b) => a.PartNumber - b.PartNumber);

                return s3Client.send(new CompleteMultipartUploadCommand({
                    Bucket: targetBucket,
                    Key: targetKey,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: parts,
                    }
                }));
            } catch (error) {
                await s3Client.send(new AbortMultipartUploadCommand({
                    Bucket: targetBucket,
                    Key: targetKey,
                    UploadId: uploadId,
                }));
                throw error;
            }
        }
    }

    async delete(bucket: string, key: string) {
        const s3Client = await this.getS3Client(bucket);

        return await s3Client.send(new DeleteObjectCommand({
            Bucket: bucket,
            Key: key,
        }));
    }

    async head(bucket: string, key: string) {
        const s3Client = await this.getS3Client(bucket);

        return s3Client.send(new HeadObjectCommand({
            Bucket: bucket,
            Key: key,
        }));
    }

    async exists(bucket: string, key: string) {
        try {
            await this.head(bucket, key);
            return true;
        } catch {
            return false;
        }
    }
}
