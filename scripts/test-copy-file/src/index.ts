import * as fs from "fs";
import * as path from "path";

import { v4 as uuidv4 } from "uuid";
import { S3Client } from "@aws-sdk/client-s3";
import { fromIni } from "@aws-sdk/credential-providers";

import { AuthProvider, mcmaApiKeyAuth, ResourceManager, ResourceManagerConfig } from "@mcma/client";
import { Job, JobParameterBag, JobProfile, JobStatus, Locator, McmaException, McmaTracker, StorageJob, Utils } from "@mcma/core";
import { buildS3Url, S3Locator } from "@mcma/aws-s3";
import { ContainerClient } from "@azure/storage-blob";
import { BlobStorageLocator, buildBlobStorageUrl } from "@mcma/azure-blob-storage";
import { S3Helper } from "./s3-helper";

const credentials = fromIni();

const JOB_PROFILE = "CopyFile";

const TERRAFORM_OUTPUT = "../../deployment/terraform.output.json";

const MEDIA_FILE = "C:/Media/2gb_file.mxf";

const s3Client = new S3Client({ credentials });

export function log(entry?: any) {
    if (typeof entry === "object") {
        console.log(JSON.stringify(entry, null, 2));
    } else {
        console.log(entry);
    }
}

async function uploadFileToContainer(containerClient: ContainerClient, filename: string) {
    const blobName = path.basename(filename);

    const blockBlobClient = containerClient.getBlockBlobClient(blobName);

    if (!await blockBlobClient.exists()) {
        console.log(`Uploading '${blobName}' to ${containerClient.containerName}`);
        await blockBlobClient.uploadFile(filename);
    }

    return new BlobStorageLocator({ url: buildBlobStorageUrl(containerClient.accountName, containerClient.containerName, blobName) });
}

async function uploadFileToBucket(bucket: string, filename: string, s3Client: S3Client) {
    const fileStream = fs.createReadStream(filename);
    fileStream.on("error", function (err) {
        console.log("File Error", err);
    });

    const s3Helper = new S3Helper({ s3ClientProvider: async (_: string) => s3Client});

    const key = path.basename(filename);

    log(`checking if file ${key} is already present`);
    if (await s3Helper.exists(bucket, key)) {
        log("Already present. Not uploading again");
    } else {
        log(`Not present. Uploading ${key} to ${bucket}`);
        await s3Helper.upload(filename, bucket, key);
    }

    const url = await buildS3Url(bucket, key, await s3Helper.getS3Client(bucket));

    return new S3Locator({ url });
}

async function waitForJobCompletion(job: Job, resourceManager: ResourceManager): Promise<Job> {
    console.log("Job is " + job.status);

    while (job.status !== JobStatus.Completed &&
           job.status !== JobStatus.Failed &&
           job.status !== JobStatus.Canceled) {

        await Utils.sleep(1000);
        job = await resourceManager.get<Job>(job.id);

        let progress = "";
        if (job.status === "Running" && job.progress) {
            progress = ` ${job.progress}%`;
        }

        console.log("Job is " + job.status + progress);
    }

    return job;
}

async function startJob(resourceManager: ResourceManager, sourceFile: Locator, destinationFile: Locator) {
    let [jobProfile] = await resourceManager.query(JobProfile, { name: JOB_PROFILE });

    // if not found bail out
    if (!jobProfile) {
        throw new McmaException(`JobProfile '${JOB_PROFILE}' not found`);
    }

    let job = new StorageJob({
        jobProfileId: jobProfile.id,
        jobInput: new JobParameterBag({
            sourceFile,
            destinationFile
        }),
        tracker: new McmaTracker({
            "id": uuidv4(),
            "label": `Test - ${JOB_PROFILE}`
        })
    });

    return resourceManager.create(job);
}

async function testJob(resourceManager: ResourceManager, sourceFile: Locator, destinationFile: Locator) {
    let job;

    if (!sourceFile) {
        log("Skipped");
        return;
    }

    console.log("Creating job");
    job = await startJob(resourceManager, sourceFile, destinationFile);

    console.log("job.id = " + job.id);
    job = await waitForJobCompletion(job, resourceManager);

    console.log(JSON.stringify(job, null, 2));
}

async function testService(resourceManager: ResourceManager, locators: { [key: string]: Locator }) {
    log("Testing copy from private S3 Bucket");
    await testJob(resourceManager, locators["awsPrivateSource"], locators["awsTarget"]);

    log("Testing copy from public S3 Bucket");
    await testJob(resourceManager, locators["awsPublicSource"], locators["awsTarget"]);

    log("Testing copy from public url");
    await testJob(resourceManager, locators["publicSource"], locators["awsTarget"]);

    log("Testing copy from private external S3 Bucket");
    await testJob(resourceManager, locators["awsPrivateExtSource"], locators["awsTarget"]);

    log("Testing copy from Azure container to S3 Bucket");
    await testJob(resourceManager, locators["azurePrivateSource"], locators["awsTarget"]);

    log("Testing copy to Azure container from private Azure container");
    await testJob(resourceManager, locators["azurePrivateSource"], locators["azureTarget"]);

    log("Testing copy from public url to to private Azure container");
    await testJob(resourceManager, locators["publicSource"], locators["azureTarget"]);

    log("Testing copy from private S3 Bucket to to private Azure container");
    await testJob(resourceManager, locators["awsPrivateSource"], locators["azureTarget"]);
}

async function main() {
    console.log("Starting test service");

    const terraformOutput = JSON.parse(fs.readFileSync(TERRAFORM_OUTPUT, "utf8"));
    const awsPublicSourceBucket: string = `${terraformOutput.deployment_prefix.value}-public-${terraformOutput.aws_region.value}`;
    const awsPrivateSourceBucket: string = `${terraformOutput.deployment_prefix.value}-private-${terraformOutput.aws_region.value}`;
    const awsPrivateExtSourceBucket: string = `${terraformOutput.deployment_prefix.value}-private-ext-${terraformOutput.aws_region.value}`;
    const awsTargetBucket: string = `${terraformOutput.deployment_prefix.value}-target-${terraformOutput.aws_region.value}`;
    const azureStorageAccountName: string = `${terraformOutput.deployment_prefix.value}-${terraformOutput.azure_location.value}`.replaceAll(new RegExp(/[^a-z0-9]+/, "g"), "").substring(0, 24);
    const azureStorageConnectionString: string = terraformOutput.storage_locations.value.azure_storage_accounts.find((sa: any) => sa.account === azureStorageAccountName).connection_string;
    const azureSourceContainer: string = "source";
    const azureTargetContainer: string = "target";

    const azureSourceContainerClient = new ContainerClient(azureStorageConnectionString, azureSourceContainer);
    const azureTargetContainerClient = new ContainerClient(azureStorageConnectionString, azureTargetContainer);

    const apiKey: string = terraformOutput.deployment_api_key.value;

    const awsResourceManagerConfig: ResourceManagerConfig = {
        serviceRegistryUrl: terraformOutput.service_registry_aws.value.service_url,
        serviceRegistryAuthType: terraformOutput.service_registry_aws.value.auth_type,
    };
    const awsResourceManager = new ResourceManager(awsResourceManagerConfig, new AuthProvider().add(mcmaApiKeyAuth({ apiKey })));

    const azureResourceManagerConfig: ResourceManagerConfig = {
        serviceRegistryUrl: terraformOutput.service_registry_azure.value.service_url,
        serviceRegistryAuthType: terraformOutput.service_registry_azure.value.auth_type,
    };
    const azureResourceManager = new ResourceManager(azureResourceManagerConfig, new AuthProvider().add(mcmaApiKeyAuth({ apiKey })));

    console.log(`Uploading media file ${MEDIA_FILE}`);
    const awsPublicSource = await uploadFileToBucket(awsPublicSourceBucket, MEDIA_FILE, s3Client);
    const publicSource = new Locator({ url: awsPublicSource.url });
    const awsPrivateSource = await uploadFileToBucket(awsPrivateSourceBucket, MEDIA_FILE, s3Client);
    const awsPrivateExtSource = await uploadFileToBucket(awsPrivateExtSourceBucket, MEDIA_FILE, s3Client);
    const azurePrivateSource = await uploadFileToContainer(azureSourceContainerClient, MEDIA_FILE);

    const awsTarget = new S3Locator({ url: await buildS3Url(awsTargetBucket, awsPrivateSource.key, s3Client) });
    const azureTarget = new BlobStorageLocator({ url: buildBlobStorageUrl(azureTargetContainerClient.accountName, azureTargetContainerClient.containerName, azurePrivateSource.blobName) });

    const locators = {
        awsPublicSource,
        publicSource,
        awsPrivateSource,
        awsPrivateExtSource,
        azurePrivateSource,
        awsTarget,
        azureTarget,
    };

    await testService(awsResourceManager, locators);
    await testService(azureResourceManager, locators);
}

main().then(() => console.log("Done")).catch(e => console.error(e));
