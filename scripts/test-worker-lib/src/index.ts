import * as path from "path";
import * as fs from "fs";
import { ConsoleLogger, LocatorStatus, Logger, Utils } from "@mcma/core";
import { ContainerClient } from "@azure/storage-blob";
import { BlobStorageLocator, buildBlobStorageUrl } from "@mcma/azure-blob-storage";
import { S3Helper } from "./s3-helper";
import { S3Client } from "@aws-sdk/client-s3";
import { buildS3Url, S3Locator } from "@mcma/aws-s3";
import { doCopyFile, SourceFile, TargetFile } from "@local/worker";
import * as mime from "mime-types";

const TERRAFORM_OUTPUT = "../../deployment/terraform.output.json";
const SMALL_FILE = "C:\\Media\\2015_GF_ORF_00_18_09_conv.mp4";
const BIG_FILE = "C:\\Media\\2gb_file.mxf";

export function log(entry?: any) {
    if (typeof entry === "object") {
        console.log(Utils.stringify(entry));
    } else {
        console.log(entry);
    }
}

const logger: Logger = new ConsoleLogger("");
logger.debug = log;
logger.info = log;
logger.warn = log;
logger.error = log;

const containerClients: { [account: string]: { [container: string]: ContainerClient } } = {};
const s3Clients: { [bucket: string]: S3Client } = {};

const getS3Client = async (bucket: string) => s3Clients[bucket];
const getContainerClient = async (account: string, container: string) => containerClients[account][container];

const s3Helper = new S3Helper({
    s3ClientProvider: getS3Client
});

let azureWestEuropeStorageAccount: { account: string, connection_string: string };
let azureEastUsContainerStorageAccount: { account: string, connection_string: string };
let s3BucketUsEast1: { bucket: string, region: string, access_key: string, secret_key: string };
let s3BucketEuWest1: { bucket: string, region: string, access_key: string, secret_key: string };

function generatePrefix() {
    return `gmam-test/${new Date().toISOString().replace(/[-:]/g, "").replace("T", "-").substring(0, 15)}/`;
}

async function uploadFileToContainer(filename: string, containerClient: ContainerClient, prefix: string) {
    const blobName = prefix + path.basename(filename);

    const blockBlobClient = containerClient.getBlockBlobClient(blobName);

    log(`checking if file ${blobName} is already present`);
    if (!await blockBlobClient.exists()) {
        console.log(`Uploading '${blobName}' to ${containerClient.containerName}`);
        await blockBlobClient.uploadFile(filename, { blobHTTPHeaders: { blobContentType: mime.lookup(blobName) || "application/octet-stream" } });
    } else {
        log("Already present. Not uploading again");
    }

    return new BlobStorageLocator({ url: buildBlobStorageUrl(containerClient.accountName, containerClient.containerName, blobName) });
}

async function uploadFileToBucket(filename: string, bucket: string, prefix: string) {
    const key = prefix + path.basename(filename);

    log(`checking if file ${key} is already present`);
    if (await s3Helper.exists(bucket, key)) {
        log("Already present. Not uploading again");
    } else {
        log("Not present. Uploading");
        await s3Helper.upload(filename, bucket, key);
    }

    const url = await buildS3Url(bucket, key, await s3Helper.getS3Client(bucket));

    return new S3Locator({ url, status: LocatorStatus.Ready });
}

async function testCopyFromS3ToS3SmallFile() {
    log("testCopyFromS3ToS3SmallFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToBucket(SMALL_FILE, s3BucketUsEast1.bucket, prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const targetFile: TargetFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.key, s3BucketEuWest1.region) }) };

    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
}

async function testCopyFromS3ToS3BigFile() {
    log("testCopyFromS3ToS3BigFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToBucket(BIG_FILE, s3BucketUsEast1.bucket, prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const targetFile: TargetFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.key, s3BucketEuWest1.region) }) };

    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
}

async function testCopyFromBlobStorageToBlobStorageSmallFile() {
    log("testCopyFromBlobStorageToBlobStorageSmallFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToContainer(SMALL_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const targetFile: TargetFile = { locator: new BlobStorageLocator({ url: buildBlobStorageUrl(azureEastUsContainerStorageAccount.account, "target", sourceLocator.blobName) }) };

    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
}


async function testCopyFromBlobStorageToBlobStorageBigFile() {
    log("testCopyFromBlobStorageToBlobStorageBigFile()");

    const prefix = "gmam-test/20231110-220458/";//generatePrefix();

    const sourceLocator = await uploadFileToContainer(BIG_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const targetFile: TargetFile = { locator: new BlobStorageLocator({ url: buildBlobStorageUrl(azureEastUsContainerStorageAccount.account, "target", sourceLocator.blobName) }) };

    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
}


async function testCopyFromBlobStorageToS3SmallFile() {
    log("testCopyFromBlobStorageToS3SmallFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToContainer(SMALL_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const targetFile: TargetFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.blobName, s3BucketEuWest1.region) }) };

    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
}

async function testCopyFromBlobStorageToS3BigFile() {
    log("testCopyFromBlobStorageToS3BigFile()");

    const prefix = "gmam-test/20231114-175359/";//generatePrefix();

    const sourceLocator = await uploadFileToContainer(BIG_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const targetFile: TargetFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.blobName, s3BucketEuWest1.region) }) };

    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
    await doCopyFile(logger, sourceFile, targetFile, getS3Client, getContainerClient);
}


async function main() {
    log("Starting test worker library");

    const terraformOutput = JSON.parse(fs.readFileSync(TERRAFORM_OUTPUT, "utf8"));

    log(terraformOutput);

    azureWestEuropeStorageAccount = terraformOutput.storage_locations.value.azure_storage_accounts.find((sa: any) => sa.account.endsWith("westeurope"));
    containerClients[azureWestEuropeStorageAccount.account] = {};
    containerClients[azureWestEuropeStorageAccount.account]["source"] = new ContainerClient(azureWestEuropeStorageAccount.connection_string, "source");
    containerClients[azureWestEuropeStorageAccount.account]["target"] = new ContainerClient(azureWestEuropeStorageAccount.connection_string, "target");

    azureEastUsContainerStorageAccount = terraformOutput.storage_locations.value.azure_storage_accounts.find((sa: any) => sa.account.endsWith("eastus"));
    containerClients[azureEastUsContainerStorageAccount.account] = {};
    containerClients[azureEastUsContainerStorageAccount.account]["source"] = new ContainerClient(azureEastUsContainerStorageAccount.connection_string, "source");
    containerClients[azureEastUsContainerStorageAccount.account]["target"] = new ContainerClient(azureEastUsContainerStorageAccount.connection_string, "target");

    s3BucketUsEast1 = terraformOutput.storage_locations.value.aws_s3_buckets.find((s: any) => s.region === "us-east-1");
    s3Clients[s3BucketUsEast1.bucket] = new S3Client({
        credentials: { accessKeyId: s3BucketUsEast1.access_key, secretAccessKey: s3BucketUsEast1.secret_key },
        region: s3BucketUsEast1.region
    });

    s3BucketEuWest1 = terraformOutput.storage_locations.value.aws_s3_buckets.find((s: any) => s.region === "eu-west-1");
    s3Clients[s3BucketEuWest1.bucket] = new S3Client({
        credentials: { accessKeyId: s3BucketEuWest1.access_key, secretAccessKey: s3BucketEuWest1.secret_key },
        region: s3BucketEuWest1.region
    });

    // await testCopyFromS3ToS3SmallFile();
    // await testCopyFromS3ToS3BigFile();

    // await testCopyFromBlobStorageToBlobStorageSmallFile()
    // await testCopyFromBlobStorageToBlobStorageBigFile();

    // await testCopyFromBlobStorageToS3SmallFile();
    await testCopyFromBlobStorageToS3BigFile();


}

main().catch(console.error);
