import * as fs from "fs";
import * as path from "path";
import * as mime from "mime-types";

import { v4 as uuidv4 } from "uuid";
import { S3Client, HeadObjectCommand, PutObjectCommand, PutObjectCommandInput } from "@aws-sdk/client-s3";
import { fromIni } from "@aws-sdk/credential-providers";

import { AuthProvider, mcmaApiKeyAuth, ResourceManager, ResourceManagerConfig } from "@mcma/client";
import { Job, JobParameterBag, JobProfile, JobStatus, Locator, McmaException, McmaTracker, StorageJob, Utils } from "@mcma/core";
import { buildS3Url, S3Locator } from "@mcma/aws-s3";
import { ContainerClient } from "@azure/storage-blob";
import { BlobStorageLocator, buildBlobStorageUrl } from "@mcma/azure-blob-storage";

const credentials = fromIni();

const JOB_PROFILE = "CopyFolder";

const TERRAFORM_OUTPUT = "../../deployment/terraform.output.json";

const MEDIA_FOLDER = "C:/Media/test/";

const s3Client = new S3Client({ credentials });

export function log(entry?: any) {
    if (typeof entry === "object") {
        console.log(JSON.stringify(entry, null, 2));
    } else {
        console.log(entry);
    }
}

function generatePrefix() {
    return `${new Date().toISOString().replace(/[-:]/g, "").replace("T", "-").substring(0, 15)}/`;
}

async function uploadFileToContainer(containerClient: ContainerClient, filename: string, prefix: string) {
    const blobName = prefix + path.basename(filename);

    const blockBlobClient = containerClient.getBlockBlobClient(blobName);

    log(`checking if file ${blobName} is already present`);
    if (!await blockBlobClient.exists()) {
        log(`Uploading '${blobName}' to ${containerClient.containerName}`);
        await blockBlobClient.uploadFile(filename, { blobHTTPHeaders: { blobContentType: mime.lookup(filename) || "application/octet-stream" }});
    }

    return new BlobStorageLocator({ url: buildBlobStorageUrl(containerClient.accountName, containerClient.containerName, blobName) });
}

async function uploadFolderToContainer(containerClient: ContainerClient, folderName: string, prefix: string) {
    const filenames = fs.readdirSync(folderName);

    for (const filename of filenames) {
        const filepath = path.join(folderName, filename);

        if (fs.lstatSync(filepath).isDirectory()) {
            await uploadFolderToContainer(containerClient, filepath, `${prefix}${filename}/`);
        } else {
            await uploadFileToContainer(containerClient, filepath, prefix);
        }
    }

    const url = buildBlobStorageUrl(containerClient.accountName, containerClient.containerName, prefix);

    return new BlobStorageLocator({ url });
}


async function uploadFileToBucket(s3Client: S3Client, filename: string, bucket: string, prefix: string) {
    const fileStream = fs.createReadStream(filename);
    fileStream.on("error", function (err) {
        log("File Error");
        log(err);
    });

    const key = prefix + path.basename(filename);

    const params: PutObjectCommandInput = {
        Bucket: bucket,
        Key: key,
        Body: fileStream,
        ContentType: mime.lookup(filename) || "application/octet-stream"
    };

    let isPresent = true;

    try {
        log(`checking if file ${key} is already present`);
        await s3Client.send(new HeadObjectCommand({ Bucket: params.Bucket, Key: params.Key }));
        log("Already present. Not uploading again");
    } catch (error) {
        isPresent = false;
    }

    if (!isPresent) {
        log("Not present. Uploading");
        await s3Client.send(new PutObjectCommand(params));
    }

    const url = !s3Client.config.endpoint ? await buildS3Url(bucket, key, s3Client) : `https://${s3Client.config.endpoint}/${bucket}/${key}`;

    return new S3Locator({ url });
}


async function uploadFolderToBucket(s3Client: S3Client, folderName: string, bucket: string, prefix: string) {
    const filenames = fs.readdirSync(folderName);

    for (const filename of filenames) {
        const filepath = path.join(folderName, filename);

        if (fs.lstatSync(filepath).isDirectory()) {
            await uploadFolderToBucket(s3Client, filepath, bucket, `${prefix}${filename}/`);
        } else {
            await uploadFileToBucket(s3Client, filepath, bucket, prefix);
        }
    }

    const url = await buildS3Url(bucket, prefix, s3Client);

    return new S3Locator({ url });
}

async function waitForJobCompletion(job: Job, resourceManager: ResourceManager): Promise<Job> {
    log("Job is " + job.status);

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

async function startJob(resourceManager: ResourceManager, sourceFolder: Locator, targetFolder: Locator) {
    let [jobProfile] = await resourceManager.query(JobProfile, { name: JOB_PROFILE });

    // if not found bail out
    if (!jobProfile) {
        throw new McmaException(`JobProfile '${JOB_PROFILE}' not found`);
    }

    let job = new StorageJob({
        jobProfileId: jobProfile.id,
        jobInput: new JobParameterBag({
            sourceFolder,
            targetFolder
        }),
        tracker: new McmaTracker({
            "id": uuidv4(),
            "label": `Test - ${JOB_PROFILE}`
        })
    });

    return resourceManager.create(job);
}

async function testJob(resourceManager: ResourceManager, sourceFolder: Locator, targetFolder: Locator) {
    let job;

    log("Creating job");
    job = await startJob(resourceManager, sourceFolder, targetFolder);

    log("job.id = " + job.id);
    job = await waitForJobCompletion(job, resourceManager);

    log(JSON.stringify(job, null, 2));
}

async function testService(resourceManager: ResourceManager, locators: { [key: string]: Locator }) {
    // log("Testing copy from private S3 Bucket");
    // await testJob(resourceManager, locators["awsPrivateSource"], locators["awsTarget"]);
    //
    // log("Testing copy from Azure container to S3 Bucket");
    // await testJob(resourceManager, locators["azurePrivateSource"], locators["awsTarget"]);

    log("Testing copy to Azure container from private Azure container");
    await testJob(resourceManager, locators["azurePrivateSource"], locators["azureTarget"]);

    // log("Testing copy from private S3 Bucket to to private Azure container");
    // await testJob(resourceManager, locators["awsPrivateSource"], locators["azureTarget"]);
}

async function main() {
    log("Starting test service");

    const terraformOutput = JSON.parse(fs.readFileSync(TERRAFORM_OUTPUT, "utf8"));
    const awsPrivateSourceBucket: string = `${terraformOutput.deployment_prefix.value}-private-${terraformOutput.aws_region.value}`;
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

    const prefix = generatePrefix();

    log(`Uploading media folder  ${MEDIA_FOLDER}`);
    const awsPrivateSource = await uploadFolderToBucket(s3Client, MEDIA_FOLDER, awsPrivateSourceBucket, prefix);
    const azurePrivateSource = await uploadFolderToContainer(azureSourceContainerClient, MEDIA_FOLDER, prefix);

    const awsTarget = new S3Locator({ url: await buildS3Url(awsTargetBucket, awsPrivateSource.key, s3Client) });
    const azureTarget = new BlobStorageLocator({ url: buildBlobStorageUrl(azureTargetContainerClient.accountName, azureTargetContainerClient.containerName, azurePrivateSource.blobName) });

    const locators = {
        awsPrivateSource,
        azurePrivateSource,
        awsTarget,
        azureTarget,
    };

    // await testService(awsResourceManager, locators);
    await testService(azureResourceManager, locators);
}

main().then(() => log("Done")).catch(e => console.error(e));
