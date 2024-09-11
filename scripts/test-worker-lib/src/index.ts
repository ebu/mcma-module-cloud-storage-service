import * as path from "path";
import * as fs from "fs";
import { ConsoleLogger, LocatorStatus, Logger, McmaException, Utils } from "@mcma/core";
import { ContainerClient } from "@azure/storage-blob";
import { BlobStorageLocator, buildBlobStorageUrl } from "@mcma/azure-blob-storage";
import { S3Helper } from "./s3-helper";
import { S3Client } from "@aws-sdk/client-s3";
import { buildS3Url, S3Locator } from "@mcma/aws-s3";
import { SourceFile, DestinationFile } from "@local/worker";
import { FileCopier } from "@local/worker";
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

const getS3Client = async (bucket: string, region?: string) => {
    switch (bucket) {
        case s3BucketEuWest1.bucket:
            return new S3Client({
                credentials: { accessKeyId: s3BucketEuWest1.access_key, secretAccessKey: s3BucketEuWest1.secret_key },
                region: region ?? s3BucketEuWest1.region
            });
        case s3BucketUsEast1.bucket:
            return new S3Client({
                credentials: { accessKeyId: s3BucketUsEast1.access_key, secretAccessKey: s3BucketUsEast1.secret_key },
                region: region ?? s3BucketUsEast1.region
            });
        default:
            throw new McmaException(`No config found for bucket '${bucket}'`);
    }
};

const getContainerClient = async (account: string, container: string) => containerClients[account][container];

const s3Helper = new S3Helper({
    s3ClientProvider: getS3Client
});

let azureWestEuropeStorageAccount: { account: string, connection_string: string };
let azureEastUsContainerStorageAccount: { account: string, connection_string: string };
let s3BucketUsEast1: { bucket: string, region: string, access_key: string, secret_key: string };
let s3BucketEuWest1: { bucket: string, region: string, access_key: string, secret_key: string };

function generatePrefix() {
    return `${new Date().toISOString().replace(/[-:]/g, "").replace("T", "-").substring(0, 15)}/`;
}

const progressUpdate = async (filesTotal: number, filesCopied: number, bytesTotal: number, bytesCopied: number) => {
    if (bytesTotal > 0) {
        const percentage = Math.round((bytesCopied / bytesTotal * 100 + Number.EPSILON) * 10) / 10;
        logger.info(`${percentage}%`);
    }
};

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
        log(`Not present. Uploading ${key} to ${bucket}`);
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

    const destinationFile: DestinationFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.key, s3BucketEuWest1.region) }) };

    const fileCopier = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });

    fileCopier.addFile(sourceFile, destinationFile);
    await fileCopier.runUntil(new Date(Date.now() + 60000), new Date(Date.now() + 120000));
}

async function testCopyFromS3ToS3BigFile() {
    log("testCopyFromS3ToS3BigFile()");

    const prefix = "20231122-150901/"; //generatePrefix();

    const sourceLocator = await uploadFileToBucket(BIG_FILE, s3BucketUsEast1.bucket, prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const destinationFile: DestinationFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.key, s3BucketEuWest1.region) }) };

    const fileCopier = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });

    fileCopier.addFile(sourceFile, destinationFile);
    await fileCopier.runUntil(new Date(Date.now() + 30000), new Date(Date.now() + 120000));
    const state = fileCopier.getState();

    log("Pausing");
    log(`${state.workItems.length} work items left`);
    log(state);

    await Utils.sleep(5000);

    const fileCopier2 = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });
    fileCopier2.setState(state);

    log("Continuing");
    await fileCopier2.runUntil(new Date(Date.now() + 60000), new Date(Date.now() + 120000));

    const state2 = fileCopier2.getState();
    log("Pausing");
    log(`${state2.workItems.length} work items left`);

}

async function testCopyFromBlobStorageToBlobStorageSmallFile() {
    log("testCopyFromBlobStorageToBlobStorageSmallFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToContainer(SMALL_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const destinationFile: DestinationFile = { locator: new BlobStorageLocator({ url: buildBlobStorageUrl(azureEastUsContainerStorageAccount.account, "target", sourceLocator.blobName) }) };

    const fileCopier = new FileCopier({
        logger,
        multipartSize: 128 * 1024 * 1024,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });

    fileCopier.addFile(sourceFile, destinationFile);
    await fileCopier.runUntil(new Date(Date.now() + 60000), new Date(Date.now() + 120000));
}


async function testCopyFromBlobStorageToBlobStorageBigFile() {
    log("testCopyFromBlobStorageToBlobStorageBigFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToContainer(BIG_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const destinationFile: DestinationFile = { locator: new BlobStorageLocator({ url: buildBlobStorageUrl(azureEastUsContainerStorageAccount.account, "target", sourceLocator.blobName) }) };

    const fileCopier = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });

    fileCopier.addFile(sourceFile, destinationFile);
    await fileCopier.runUntil(new Date(Date.now() + 30000), new Date(Date.now() + 120000));

    await Utils.sleep(5000);
    const state = fileCopier.getState();

    log("Pausing");
    log(`${state.workItems.length} work items left`);
    log(state);

    const fileCopier2 = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });
    fileCopier2.setState(state);

    log("Continuing");
    await fileCopier2.runUntil(new Date(Date.now() + 60000), new Date(Date.now() + 120000));

    const state2 = fileCopier2.getState();
    log("Pausing");
    log(`${state2.workItems.length} work items left`);
}

async function testCopyFromBlobStorageToS3SmallFile() {
    log("testCopyFromBlobStorageToS3SmallFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToContainer(SMALL_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const destinationFile: DestinationFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.blobName, s3BucketEuWest1.region) }) };

    const fileCopier = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });

    fileCopier.addFile(sourceFile, destinationFile);
    await fileCopier.runUntil(new Date(Date.now() + 60000), new Date(Date.now() + 120000));
}

async function testCopyFromBlobStorageToS3BigFile() {
    log("testCopyFromBlobStorageToS3BigFile()");

    const prefix = generatePrefix();

    const sourceLocator = await uploadFileToContainer(BIG_FILE, containerClients[azureWestEuropeStorageAccount.account]["source"], prefix);
    const sourceFile: SourceFile = { locator: sourceLocator };

    const destinationFile: DestinationFile = { locator: new S3Locator({ url: await buildS3Url(s3BucketEuWest1.bucket, sourceLocator.blobName, s3BucketEuWest1.region) }) };

    const fileCopier = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });

    fileCopier.addFile(sourceFile, destinationFile);
    await fileCopier.runUntil(new Date(Date.now() + 30000), new Date(Date.now() + 120000));

    await Utils.sleep(5000);
    const state = fileCopier.getState();

    log("Pausing");
    log(`${state.workItems.length} work items left`);
    log(state);

    const fileCopier2 = new FileCopier({
        logger,
        maxConcurrency: 8,
        getS3Client,
        getContainerClient,
        progressUpdate,
    });
    fileCopier2.setState(state);

    log("Continuing");
    await fileCopier2.runUntil(new Date(Date.now() + 60000), new Date(Date.now() + 120000));

    const state2 = fileCopier2.getState();
    log("Pausing");
    log(`${state2.workItems.length} work items left`);
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
    s3BucketEuWest1 = terraformOutput.storage_locations.value.aws_s3_buckets.find((s: any) => s.region === "eu-west-1");

    // await testCopyFromS3ToS3SmallFile();
    // await testCopyFromS3ToS3BigFile();

    await testCopyFromBlobStorageToBlobStorageSmallFile();
    // await testCopyFromBlobStorageToBlobStorageBigFile();

    // await testCopyFromBlobStorageToS3SmallFile();
    // await testCopyFromBlobStorageToS3BigFile();
}

main().then(() => log("Done")).catch(console.error);
