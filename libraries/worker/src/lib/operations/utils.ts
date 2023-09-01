import { Locator } from "@mcma/core";
import { S3Locator } from "@mcma/aws-s3";
import { BlobStorageLocator } from "@mcma/azure-blob-storage";

export function isS3Locator(x: Locator): x is S3Locator {
    return typeof x === "object" && x["@type"] === "S3Locator";
}

export function isBlobStorageLocator(x: Locator): x is BlobStorageLocator {
    return typeof x === "object" && x["@type"] === "BlobStorageLocator";
}
