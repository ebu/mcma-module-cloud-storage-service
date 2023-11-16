import { S3Locator } from "@mcma/aws-s3";
import {
    AbortMultipartUploadCommand,
    CompletedPart,
    CompleteMultipartUploadCommand,
    CreateMultipartUploadCommand,
    PutObjectCommand,
    S3Client,
    UploadPartCommand,
    UploadPartCommandOutput,
} from "@aws-sdk/client-s3";
import { AxiosRequestConfig, default as axios } from "axios";
import { Logger, McmaException } from "@mcma/core";
import { ObjectData } from "./model";

let AWS_URL_COPY_MAX_CONCURRENCY= Number.parseInt(process.env.AWS_URL_COPY_MAX_CONCURRENCY);
if (!Number.isSafeInteger(AWS_URL_COPY_MAX_CONCURRENCY) || AWS_URL_COPY_MAX_CONCURRENCY <= 0) {
    AWS_URL_COPY_MAX_CONCURRENCY = 8;
}
const MULTIPART_SIZE = 64 * 1024 * 1024;

interface MultiPartUploadRequest {
    input: {
        partNumber: number,
        start: number,
        end: number,
    };
    promise?: Promise<UploadPartCommandOutput>;
    output?: UploadPartCommandOutput;
    error?: any;
}

export async function copyFileFromUrlToS3(logger: Logger, sourceUrl: string, sourceObjectData: ObjectData, target: S3Locator, s3Client: S3Client, axiosConfig?: AxiosRequestConfig) {

    logger.info(sourceObjectData);

    const objectSize = sourceObjectData.size;

    if (objectSize < MULTIPART_SIZE * 2) {
        logger.info("Single PUT");

        logger.info(`Downloading from ${sourceUrl}`);
        const response = await axios.get(sourceUrl, Object.assign({}, axiosConfig, { responseType: "arraybuffer" }));
        logger.info("Uploading");
        return await s3Client.send(new PutObjectCommand({
            Bucket: target.bucket,
            Key: target.key,
            Body: response.data,
            ContentType: sourceObjectData.contentType
        }));
    } else {
        logger.info("multipart upload");

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
            const preparedRequests: MultiPartUploadRequest[] = [];
            const uploadingRequests: MultiPartUploadRequest[] = [];
            const finishedRequests: MultiPartUploadRequest[] = [];
            const errorRequests: MultiPartUploadRequest[] = [];

            let bytePosition = 0;
            for (let i = 1; bytePosition < objectSize; i++) {
                const start = bytePosition;
                const end = (bytePosition + multipartSize - 1 >= objectSize ? objectSize - 1 : bytePosition + multipartSize - 1);

                preparedRequests.push({
                    input: {
                        partNumber: i,
                        start,
                        end,
                    }
                });

                bytePosition += multipartSize;
            }

            while (preparedRequests.length > 0) {
                if (uploadingRequests.length >= AWS_URL_COPY_MAX_CONCURRENCY) {
                    await processMultiPartUploadRequest(uploadingRequests, finishedRequests, errorRequests);
                }

                const request = preparedRequests.shift();
                request.promise = new Promise<UploadPartCommandOutput>(async (resolve, reject) => {
                    try {
                        logger.info("Starting partNumber " + request.input.partNumber);

                        const headers = Object.assign({}, axiosConfig?.headers, { Range: `bytes=${request.input.start}-${request.input.end}` });
                        const response = await axios.get(sourceUrl, Object.assign({}, axiosConfig, { responseType: "arraybuffer", headers }));

                        resolve(s3Client.send(new UploadPartCommand({
                            Bucket: target.bucket,
                            Key: target.key,
                            UploadId: uploadId,
                            Body: response.data,
                            PartNumber: request.input.partNumber
                        })));
                    } catch (error) {
                        reject(error);
                    }
                });
                uploadingRequests.push(request);
            }

            while (uploadingRequests.length > 0) {
                await processMultiPartUploadRequest(uploadingRequests, finishedRequests, errorRequests);
            }

            if (errorRequests.length > 0) {
                logger.info(errorRequests[0].error);
                throw new McmaException("Transfer failed with " + errorRequests.length + " errors", errorRequests[0].error);
            }

            const parts: CompletedPart[] = finishedRequests.map(request => {
                return {
                    ETag: request.output.ETag,
                    PartNumber: request.input.partNumber,
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

async function processMultiPartUploadRequest(uploadingRequests: MultiPartUploadRequest[], finishedRequests: MultiPartUploadRequest[], errorRequests: MultiPartUploadRequest[]) {
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
