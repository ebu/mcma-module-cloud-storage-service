import * as fs from "fs";
import * as path from "path";
import { S3Client, StorageClass, } from "@aws-sdk/client-s3";
import { fromIni } from "@aws-sdk/credential-providers";
import { Job, JobParameterBag, JobProfile, JobStatus, Locator, McmaException, McmaTracker, StorageJob, Utils } from "@mcma/core";
import { S3Locator, buildS3Url } from "@mcma/aws-s3";
import { S3Helper } from "./s3-helper";
import { AuthProvider, ResourceManager, ResourceManagerConfig, mcmaApiKeyAuth } from "@mcma/client";
import { v4 as uuidv4 } from "uuid";

import { RestorePriority } from "@local/storage";

const JOB_PROFILE = "RestoreFiles";
const TERRAFORM_OUTPUT = "../../deployment/terraform.output.json";
const MEDIA_FILE = "C:/Media/2gb_file.mxf";

const credentials = fromIni();
const s3Client = new S3Client({ credentials });

export function log(entry?: any) {
    if (typeof entry === "object") {
        console.log(Utils.stringify(entry));
    } else {
        console.log(entry);
    }
}

async function uploadFileToBucket(bucket: string, filename: string, prefix: string, s3Client: S3Client) {
    const fileStream = fs.createReadStream(filename);
    fileStream.on("error", function (err: any) {
        log("File Error");
        log(err);
    });

    const s3Helper = new S3Helper({ s3ClientProvider: async (_: string) => s3Client });

    const key = prefix + path.basename(filename);

    log(`checking if file ${key} is already present`);
    if (await s3Helper.exists(bucket, key)) {
        log("Already present. Not uploading again");
    } else {
        log(`Not present. Uploading ${key} to ${bucket}`);
        await s3Helper.upload(filename, bucket, key, StorageClass.GLACIER);
    }

    const url = await buildS3Url(bucket, key, await s3Helper.getS3Client(bucket));

    return new S3Locator({ url });
}

function generatePrefix() {
    return `${new Date().toISOString().replace(/[-:]/g, "").replace("T", "-").substring(0, 15)}/`;
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

        log("Job is " + job.status + progress);
    }

    return job;
}

async function startJob(resourceManager: ResourceManager, locators: Locator[], priority: RestorePriority) {
    let [jobProfile] = await resourceManager.query(JobProfile, { name: JOB_PROFILE });

    // if not found bail out
    if (!jobProfile) {
        throw new McmaException(`JobProfile '${JOB_PROFILE}' not found`);
    }

    let job = new StorageJob({
        jobProfileId: jobProfile.id,
        jobInput: new JobParameterBag({
            locators,
            priority,
        }),
        tracker: new McmaTracker({
            "id": uuidv4(),
            "label": `Test - ${JOB_PROFILE}`
        })
    });

    return resourceManager.create(job);
}

async function testJob(resourceManager: ResourceManager, files: Locator[], priority: RestorePriority) {
    let job;

    if (!files) {
        log("Skipped");
        return;
    }

    log("Creating job");
    job = await startJob(resourceManager, files, priority);

    log("job.id = " + job.id);
    job = await waitForJobCompletion(job, resourceManager);

    log(job);
}


async function main() {
    log("Starting test service");

    const terraformOutput = JSON.parse(fs.readFileSync(TERRAFORM_OUTPUT, "utf8"));

    const apiKey: string = terraformOutput.deployment_api_key.value;

    const awsResourceManagerConfig: ResourceManagerConfig = {
        serviceRegistryUrl: terraformOutput.service_registry_aws.value.service_url,
        serviceRegistryAuthType: terraformOutput.service_registry_aws.value.auth_type,
    };
    const awsResourceManager = new ResourceManager(awsResourceManagerConfig, new AuthProvider().add(mcmaApiKeyAuth({ apiKey })));

    const awsArchiveBucket: string = `${terraformOutput.deployment_prefix.value}-archive-${terraformOutput.aws_region.value}`;
    const prefix = generatePrefix();
    const awsArchiveFile1 = await uploadFileToBucket(awsArchiveBucket, MEDIA_FILE, prefix + "1-", s3Client);
    const awsArchiveFile2 = await uploadFileToBucket(awsArchiveBucket, MEDIA_FILE, prefix + "2-", s3Client);
    const locators = [awsArchiveFile1, awsArchiveFile2];

    log(locators);

    await testJob(awsResourceManager, locators, RestorePriority.High);
}

main().then(() => log("Done")).catch(e => console.error(e));
