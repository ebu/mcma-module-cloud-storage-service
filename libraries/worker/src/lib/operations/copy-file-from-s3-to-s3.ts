import { S3Locator } from "@mcma/aws-s3";
import {
    AbortMultipartUploadCommand,
    CompletedPart,
    CompleteMultipartUploadCommand,
    CopyObjectCommand,
    CreateMultipartUploadCommand,
    S3Client,
    UploadPartCopyCommand, UploadPartCopyCommandInput, UploadPartCopyCommandOutput
} from "@aws-sdk/client-s3";
import { ObjectData } from "./model";
import { Logger } from "@mcma/core";

let AWS_S3_COPY_MAX_CONCURRENCY= Number.parseInt(process.env.AWS_S3_COPY_MAX_CONCURRENCY);
if (!Number.isSafeInteger(AWS_S3_COPY_MAX_CONCURRENCY) || AWS_S3_COPY_MAX_CONCURRENCY <= 0) {
    AWS_S3_COPY_MAX_CONCURRENCY = 16;
}
const MULTIPART_SIZE = 64 * 1024 * 1024;

interface MultiPartCopyRequest {
    input: UploadPartCopyCommandInput;
    promise?: Promise<UploadPartCopyCommandOutput>;
    output?: UploadPartCopyCommandOutput;
    error?: any;
}

export async function copyFileFromS3ToS3(logger: Logger, source: S3Locator, sourceObjectData: ObjectData, target: S3Locator, s3Client: S3Client) {
    const objectSize = sourceObjectData.size;


    if (objectSize < MULTIPART_SIZE * 2) {
        logger.info("Single copy");

        return await s3Client.send(new CopyObjectCommand({
            Bucket: target.bucket,
            Key: target.key,
            CopySource: `/${source.bucket}/${source.key}`,
            ContentType: sourceObjectData.contentType
        }));
    } else {
        logger.info("multipart copy");

        let multipartSize = MULTIPART_SIZE;
        while (objectSize / 10000 > multipartSize) {
            multipartSize *= 2;
        }

        const createResponse = await s3Client.send(new CreateMultipartUploadCommand({
            Bucket: target.bucket,
            Key: target.key,
            ContentType: sourceObjectData.contentType
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
                const end = (bytePosition + multipartSize - 1 >= objectSize ? objectSize - 1 : bytePosition + multipartSize - 1);

                preparedRequests.push({
                    input: {
                        Bucket: target.bucket,
                        Key: target.key,
                        CopySource: encodeURI(source.bucket + "/" + source.key),
                        CopySourceRange: "bytes=" + start + "-" + end,
                        PartNumber: i,
                        UploadId: uploadId,
                    }
                });

                bytePosition += multipartSize;
            }

            while (preparedRequests.length > 0) {
                if (uploadingRequests.length >= AWS_S3_COPY_MAX_CONCURRENCY) {
                    await processMultiPartCopyRequest(uploadingRequests, finishedRequests, errorRequests);
                }

                const request = preparedRequests.shift();
                logger.info("Starting partNumber " + request.input.PartNumber);
                request.promise = s3Client.send(new UploadPartCopyCommand(request.input));
                uploadingRequests.push(request);
            }

            while (uploadingRequests.length > 0) {
                await processMultiPartCopyRequest(uploadingRequests, finishedRequests, errorRequests);
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
                Bucket: target.bucket,
                Key: target.key,
                UploadId: uploadId,
                MultipartUpload: {
                    Parts: parts,
                }
            }));
        } catch (error) {
            await s3Client.send(new AbortMultipartUploadCommand({
                Bucket: target.bucket,
                Key: target.key,
                UploadId: uploadId,
            }));
            throw error;
        }
    }
}

async function processMultiPartCopyRequest(uploadingRequests: MultiPartCopyRequest[], finishedRequests: MultiPartCopyRequest[], errorRequests: MultiPartCopyRequest[]) {
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
