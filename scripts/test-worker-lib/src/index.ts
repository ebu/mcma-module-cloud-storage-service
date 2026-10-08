import * as fs from "fs";
import * as path from "path";
import { fork } from "child_process";
import { createHash, randomFillSync } from "crypto";
import { once } from "events";
import { Readable } from "stream";

import { GetObjectCommand, HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ContainerClient } from "@azure/storage-blob";
import { BlobStorageLocator, buildBlobStorageUrl } from "@mcma/azure-blob-storage";
import { buildS3Url, S3Locator } from "@mcma/aws-s3";
import { ConsoleLogger, Locator, LocatorStatus, Logger, McmaException, Utils } from "@mcma/core";
import * as mime from "mime-types";

import { DestinationFile, FileCopier, FileCopierState, SourceFile, UrlTrie } from "@local/storage";
import { S3Helper } from "./s3-helper";

const TERRAFORM_OUTPUT = path.resolve(__dirname, "../../../../deployment/terraform.output.json");
const DEFAULT_TEST_FILE_SIZE = 2_000_000_000;
const CRASH_EXIT_CODE = 86;
const MIN_MULTIPART_FILE_SIZE = 5 * 1024 * 1024;
const ALL_ROUTES: TestRoute[] = ["azure-to-s3", "s3-to-azure", "azure-to-azure", "s3-to-s3"];

type CloudProvider = "azure" | "s3";
type TestRoute = "azure-to-s3" | "s3-to-azure" | "azure-to-azure" | "s3-to-s3";
type TestPhase = "capture" | "resume";

interface TestManifest {
    route: TestRoute;
    runId: string;
    sourceFilePath: string;
    sourceSize: number;
    sourceSha256: string;
    sourceUrl: string;
    destinationUrl: string;
    stateDirectory: string;
}

interface StorageAccountConfig {
    account: string;
    connection_string: string;
}

interface S3BucketConfig {
    bucket: string;
    region: string;
    access_key: string;
    secret_key: string;
}

let azurePrimaryStorageAccount: StorageAccountConfig;
let azureEastUsStorageAccount: StorageAccountConfig;
let s3BucketUsEast1: S3BucketConfig;
let s3BucketEuWest1: S3BucketConfig;

const containerClients: Record<string, Record<string, ContainerClient>> = {};

function log(entry?: unknown) {
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

const getS3Client = async (bucket: string, region?: string) => {
    let config: S3BucketConfig;
    if (bucket === s3BucketEuWest1.bucket) {
        config = s3BucketEuWest1;
    } else if (bucket === s3BucketUsEast1.bucket) {
        config = s3BucketUsEast1;
    } else {
        throw new McmaException(`No configuration found for S3 bucket '${bucket}'`);
    }

    return new S3Client({
        credentials: {
            accessKeyId: config.access_key,
            secretAccessKey: config.secret_key,
        },
        region: region ?? config.region,
        requestStreamBufferSize: 65_536,
    });
};

const getContainerClient = async (account: string, container: string) => {
    const client = containerClients[account]?.[container];
    if (!client) {
        throw new McmaException(`No configuration found for Azure container '${account}/${container}'`);
    }
    return client;
};

const s3Helper = new S3Helper({ s3ClientProvider: getS3Client });

function requiredValue<T>(value: T | undefined, description: string): T {
    if (!value) {
        throw new McmaException(`${description} was not found in ${TERRAFORM_OUTPUT}`);
    }
    return value;
}

function buildAzureStorageAccountName(deploymentPrefix: string, location: string) {
    return `${deploymentPrefix}-${location}`.replace(/[^a-z0-9]+/g, "").substring(0, 24);
}

function initializeCloudClients() {
    const terraformOutput = JSON.parse(fs.readFileSync(TERRAFORM_OUTPUT, "utf8"));
    const storageLocations = terraformOutput.storage_locations?.value;
    if (!storageLocations) {
        throw new McmaException(`storage_locations.value was not found in ${TERRAFORM_OUTPUT}`);
    }

    const deploymentPrefix = requiredValue<string>(terraformOutput.deployment_prefix?.value, "deployment_prefix.value");
    const azureLocation = requiredValue<string>(terraformOutput.azure_location?.value, "azure_location.value");
    const primaryStorageAccountName = buildAzureStorageAccountName(deploymentPrefix, azureLocation);
    const eastUsStorageAccountName = buildAzureStorageAccountName(deploymentPrefix, "eastus");

    azurePrimaryStorageAccount = requiredValue(
        storageLocations.azure_storage_accounts.find((account: StorageAccountConfig) => account.account === primaryStorageAccountName),
        `${azureLocation} Azure storage account '${primaryStorageAccountName}'`,
    );
    azureEastUsStorageAccount = requiredValue(
        storageLocations.azure_storage_accounts.find((account: StorageAccountConfig) => account.account === eastUsStorageAccountName),
        `East US Azure storage account '${eastUsStorageAccountName}'`,
    );
    s3BucketUsEast1 = requiredValue(
        storageLocations.aws_s3_buckets.find((bucket: S3BucketConfig) => bucket.region === "us-east-1"),
        "us-east-1 S3 bucket",
    );
    s3BucketEuWest1 = requiredValue(
        storageLocations.aws_s3_buckets.find((bucket: S3BucketConfig) => bucket.region === "eu-west-1"),
        "eu-west-1 S3 bucket",
    );

    for (const storageAccount of [azurePrimaryStorageAccount, azureEastUsStorageAccount]) {
        containerClients[storageAccount.account] = {
            source: new ContainerClient(storageAccount.connection_string, "source"),
            target: new ContainerClient(storageAccount.connection_string, "target"),
        };
    }
}

function readNumberSetting(name: string, defaultValue: number): number {
    const rawValue = process.env[name];
    if (!rawValue) {
        return defaultValue;
    }

    const value = Number(rawValue);
    if (!Number.isFinite(value) || value <= 0) {
        throw new McmaException(`${name} must be a positive number`);
    }
    return value;
}

function readIntegerSetting(name: string, defaultValue: number): number {
    const value = readNumberSetting(name, defaultValue);
    if (!Number.isSafeInteger(value)) {
        throw new McmaException(`${name} must be a positive integer`);
    }
    return value;
}

function parseRoute(value: string): TestRoute {
    switch (value) {
        case "azure-to-s3":
        case "s3-to-azure":
        case "azure-to-azure":
        case "s3-to-s3":
            return value;
        default:
            throw new McmaException(`Unsupported FILE_COPIER_TEST_ROUTE '${value}'`);
    }
}

function getRoutes(): TestRoute[] {
    const configuredRoutes = process.env.FILE_COPIER_TEST_ROUTES ?? process.env.FILE_COPIER_TEST_ROUTE;
    if (!configuredRoutes) {
        return ALL_ROUTES;
    }

    const routes = configuredRoutes.split(",").map(value => parseRoute(value.trim()));
    if (routes.length === 0) {
        throw new McmaException("FILE_COPIER_TEST_ROUTES must contain at least one route");
    }
    return routes;
}

function getProviders(route: TestRoute): { source: CloudProvider, destination: CloudProvider } {
    const [source, , destination] = route.split("-") as [CloudProvider, "to", CloudProvider];
    return { source, destination };
}

function getArgument(name: string): string | undefined {
    const prefix = `--${name}=`;
    return process.argv.find(argument => argument.startsWith(prefix))?.substring(prefix.length);
}

async function calculateSha256(stream: Readable, label: string, totalBytes?: number): Promise<string> {
    const hash = createHash("sha256");
    let bytesRead = 0;
    let nextProgressLog = 256 * 1024 * 1024;

    for await (const chunk of stream) {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        hash.update(buffer);
        bytesRead += buffer.length;
        if (bytesRead >= nextProgressLog) {
            const progress = totalBytes ? ` (${Math.round(bytesRead / totalBytes * 100)}%)` : "";
            log(`${label}: hashed ${bytesRead} bytes${progress}`);
            nextProgressLog += 256 * 1024 * 1024;
        }
    }

    return hash.digest("hex");
}

async function generateRandomFile(filename: string, size: number): Promise<string> {
    await fs.promises.mkdir(path.dirname(filename), { recursive: true });
    const file = await fs.promises.open(filename, "w");
    const hash = createHash("sha256");
    const buffer = Buffer.allocUnsafe(8 * 1024 * 1024);
    let bytesWritten = 0;
    let nextProgressLog = 256 * 1024 * 1024;

    log(`Generating ${size} bytes of random data at ${filename}`);
    try {
        while (bytesWritten < size) {
            const length = Math.min(buffer.length, size - bytesWritten);
            randomFillSync(buffer, 0, length);
            await file.write(buffer, 0, length);
            hash.update(buffer.subarray(0, length));
            bytesWritten += length;

            if (bytesWritten >= nextProgressLog) {
                log(`Generated ${bytesWritten}/${size} bytes (${Math.round(bytesWritten / size * 100)}%)`);
                nextProgressLog += 256 * 1024 * 1024;
            }
        }
    } finally {
        await file.close();
    }

    return hash.digest("hex");
}

async function prepareTestFile(): Promise<{ filename: string, size: number, sha256: string }> {
    const configuredFilename = process.env.FILE_COPIER_TEST_FILE;
    const filename = path.resolve(configuredFilename ?? path.resolve(__dirname, "../test-data/random-2gb.bin"));
    const requestedSize = readIntegerSetting("FILE_COPIER_TEST_FILE_SIZE", DEFAULT_TEST_FILE_SIZE);
    const regenerate = process.env.FILE_COPIER_TEST_REGENERATE === "true";
    let stats: fs.Stats | undefined;

    try {
        stats = await fs.promises.stat(filename);
    } catch (error: any) {
        if (error?.code !== "ENOENT") {
            throw error;
        }
    }

    let sha256: string;
    if (!configuredFilename && (regenerate || !stats?.isFile() || stats.size !== requestedSize)) {
        sha256 = await generateRandomFile(filename, requestedSize);
        stats = await fs.promises.stat(filename);
    } else {
        if (!stats?.isFile()) {
            throw new McmaException(`FILE_COPIER_TEST_FILE is not a file: ${filename}`);
        }
        log(`Calculating SHA-256 for ${filename}`);
        sha256 = await calculateSha256(fs.createReadStream(filename), "Source", stats.size);
    }

    if (stats.size <= MIN_MULTIPART_FILE_SIZE) {
        throw new McmaException(`The test file must be larger than ${MIN_MULTIPART_FILE_SIZE} bytes to exercise multipart recovery`);
    }
    log(`Test file SHA-256: ${sha256}`);
    return { filename, size: stats.size, sha256 };
}

async function uploadFileToAzure(filename: string, sha256: string): Promise<BlobStorageLocator> {
    const container = containerClients[azurePrimaryStorageAccount.account].source;
    const blobName = `file-copier-test/source/${sha256}/${path.basename(filename)}`;
    const blob = container.getBlockBlobClient(blobName);
    const localSize = (await fs.promises.stat(filename)).size;

    let upload = true;
    if (await blob.exists()) {
        upload = (await blob.getProperties()).contentLength !== localSize;
    }

    if (upload) {
        log(`Uploading ${localSize} bytes to Azure source '${container.containerName}/${blobName}'`);
        await blob.uploadFile(filename, {
            blobHTTPHeaders: { blobContentType: mime.lookup(blobName) || "application/octet-stream" },
        });
    } else {
        log(`Reusing Azure source '${container.containerName}/${blobName}'`);
    }

    return new BlobStorageLocator({ url: buildBlobStorageUrl(container.accountName, container.containerName, blobName) });
}

async function uploadFileToS3(filename: string, sha256: string): Promise<S3Locator> {
    const key = `file-copier-test/source/${sha256}/${path.basename(filename)}`;
    const localSize = (await fs.promises.stat(filename)).size;
    let upload = true;

    if (await s3Helper.exists(s3BucketUsEast1.bucket, key)) {
        upload = (await s3Helper.head(s3BucketUsEast1.bucket, key)).ContentLength !== localSize;
    }

    if (upload) {
        log(`Uploading ${localSize} bytes to S3 source '${s3BucketUsEast1.bucket}/${key}'`);
        await s3Helper.upload(filename, s3BucketUsEast1.bucket, key);
    } else {
        log(`Reusing S3 source '${s3BucketUsEast1.bucket}/${key}'`);
    }

    return new S3Locator({
        url: await buildS3Url(s3BucketUsEast1.bucket, key, s3BucketUsEast1.region),
        status: LocatorStatus.Ready,
    });
}

async function buildManifest(route: TestRoute, suiteRunId: string, testFile: { filename: string, size: number, sha256: string }): Promise<TestManifest> {
    const runId = `${suiteRunId}-${route}`;
    const stateDirectory = path.resolve(__dirname, "../live-checkpoint", runId);
    await fs.promises.mkdir(stateDirectory, { recursive: true });

    const providers = getProviders(route);
    const source = providers.source === "azure"
        ? await uploadFileToAzure(testFile.filename, testFile.sha256)
        : await uploadFileToS3(testFile.filename, testFile.sha256);
    const destinationName = `file-copier-test/runs/${runId}/${path.basename(testFile.filename)}`;
    const destinationUrl = providers.destination === "azure"
        ? buildBlobStorageUrl(azureEastUsStorageAccount.account, "target", destinationName)
        : await buildS3Url(s3BucketEuWest1.bucket, destinationName, s3BucketEuWest1.region);

    const manifest: TestManifest = {
        route,
        runId,
        sourceFilePath: testFile.filename,
        sourceSize: testFile.size,
        sourceSha256: testFile.sha256,
        sourceUrl: source.url,
        destinationUrl,
        stateDirectory,
    };
    await fs.promises.writeFile(path.join(stateDirectory, "manifest.json"), JSON.stringify(manifest, null, 2));
    return manifest;
}

function createLocator(provider: CloudProvider, url: string): Locator {
    return provider === "azure"
        ? new BlobStorageLocator({ url })
        : new S3Locator({ url, status: LocatorStatus.Ready });
}

function statePaths(manifest: TestManifest) {
    return {
        json: path.join(manifest.stateDirectory, "state.json"),
        trie: path.join(manifest.stateDirectory, "state.trie"),
    };
}

async function saveState(manifest: TestManifest, state: FileCopierState) {
    const filenames = statePaths(manifest);
    const trieStream = fs.createWriteStream(filenames.trie);
    const trieFinished = once(trieStream, "finish");
    await state.trie.serialize(trieStream);
    trieStream.end();
    await trieFinished;

    await fs.promises.writeFile(filenames.json, JSON.stringify({
        filesTotal: state.filesTotal,
        filesCopied: state.filesCopied,
        bytesTotal: state.bytesTotal,
        bytesCopied: state.bytesCopied,
        workItems: state.workItems,
    }, null, 2));
}

async function loadState(manifest: TestManifest): Promise<FileCopierState> {
    const filenames = statePaths(manifest);
    const state = JSON.parse(await fs.promises.readFile(filenames.json, "utf8"), Utils.reviver);
    return {
        filesTotal: state.filesTotal,
        filesCopied: state.filesCopied,
        bytesTotal: state.bytesTotal,
        bytesCopied: state.bytesCopied,
        workItems: state.workItems,
        trie: await UrlTrie.deserialize(fs.createReadStream(filenames.trie)),
    };
}

function summarizeState(state: FileCopierState) {
    const workByType: Record<string, number> = {};
    for (const workItem of state.workItems) {
        workByType[workItem.type] = (workByType[workItem.type] ?? 0) + 1;
    }
    return {
        files: `${state.filesCopied}/${state.filesTotal}`,
        bytes: `${state.bytesCopied}/${state.bytesTotal}`,
        workItems: state.workItems.length,
        workByType,
    };
}

function buildFileCopier(checkpointUpdate: (state: FileCopierState) => Promise<void>) {
    return new FileCopier({
        logger,
        maxConcurrency: readNumberSetting("FILE_COPIER_TEST_CONCURRENCY", 2),
        multipartSize: readNumberSetting("FILE_COPIER_TEST_MULTIPART_SIZE", MIN_MULTIPART_FILE_SIZE),
        multipartSegmentBatchSize: 8,
        checkpointInterval: readNumberSetting("FILE_COPIER_TEST_CHECKPOINT_INTERVAL", 2_000),
        getS3Client,
        getContainerClient,
        checkpointUpdate,
        progressUpdate: async (filesTotal, filesCopied, bytesTotal, bytesCopied) => {
            if (bytesTotal > 0) {
                const percentage = Math.round(bytesCopied / bytesTotal * 1_000) / 10;
                process.stdout.write(`Progress ${percentage}% (${filesCopied}/${filesTotal} files)\r`);
            }
        },
        debug: process.env.FILE_COPIER_TEST_DEBUG === "true",
    });
}

async function runPhase(phase: TestPhase, manifestPath: string, shouldCrash: boolean, crashNumber: number) {
    initializeCloudClients();
    const manifest: TestManifest = JSON.parse(await fs.promises.readFile(manifestPath, "utf8"));
    const providers = getProviders(manifest.route);
    const restoredState = phase === "resume" ? await loadState(manifest) : undefined;
    const startingBytesCopied = restoredState?.bytesCopied ?? 0;
    let checkpointNumber = 0;

    const copier = buildFileCopier(async state => {
        checkpointNumber++;
        await saveState(manifest, state);
        log(`\n${phase} checkpoint ${checkpointNumber}: ${Utils.stringify(summarizeState(state))}`);

        if (shouldCrash && state.bytesTotal > 0 && state.bytesCopied > startingBytesCopied && state.filesCopied < state.filesTotal && state.workItems.length > 0) {
            log(`Checkpoint saved. Simulating abrupt process termination ${crashNumber} with ${state.workItems.length} unfinished work items.`);
            process.exit(CRASH_EXIT_CODE);
        }
    });

    if (phase === "capture") {
        const sourceFile: SourceFile = { locator: createLocator(providers.source, manifest.sourceUrl) };
        const destinationFile: DestinationFile = { locator: createLocator(providers.destination, manifest.destinationUrl) };
        copier.addFile(sourceFile, destinationFile);
    } else {
        log(`Restoring checkpoint: ${Utils.stringify(summarizeState(restoredState))}`);
        await copier.setState(restoredState);
    }

    const maxRunMilliseconds = readNumberSetting("FILE_COPIER_TEST_MAX_MINUTES", 30) * 60_000;
    await copier.runUntil(
        new Date(Date.now() + maxRunMilliseconds),
        new Date(Date.now() + maxRunMilliseconds + 60_000),
    );

    const error = copier.getError();
    if (error) {
        throw error;
    }

    const finalState = await copier.getState();
    await saveState(manifest, finalState);
    if (shouldCrash) {
        throw new McmaException("Copy completed before an in-progress checkpoint could be captured. Use a larger file or lower concurrency.");
    }
    if (finalState.workItems.length > 0 || finalState.filesCopied !== finalState.filesTotal) {
        throw new McmaException(`Recovery did not finish all work: ${Utils.stringify(summarizeState(finalState))}`);
    }

    await verifyDestination(manifest, providers.destination);
    log(`Recovery completed successfully: ${Utils.stringify(summarizeState(finalState))}`);
}

async function verifyDestination(manifest: TestManifest, provider: CloudProvider) {
    const expectedLength = manifest.sourceSize;
    let actualLength: number | undefined;
    let destinationStream: Readable | undefined;

    if (provider === "azure") {
        const locator = new BlobStorageLocator({ url: manifest.destinationUrl });
        const container = await getContainerClient(locator.account, locator.container);
        const blob = container.getBlockBlobClient(locator.blobName);
        actualLength = (await blob.getProperties()).contentLength;
        destinationStream = (await blob.download()).readableStreamBody as Readable;
    } else {
        const locator = new S3Locator({ url: manifest.destinationUrl });
        const client = await getS3Client(locator.bucket, locator.region);
        actualLength = (await client.send(new HeadObjectCommand({ Bucket: locator.bucket, Key: locator.key }))).ContentLength;
        destinationStream = (await client.send(new GetObjectCommand({ Bucket: locator.bucket, Key: locator.key }))).Body as Readable;
    }

    if (actualLength !== expectedLength) {
        throw new McmaException(`Destination size mismatch. Expected ${expectedLength}, got ${actualLength}`);
    }
    log(`Verified destination size: ${actualLength} bytes`);

    if (!destinationStream) {
        throw new McmaException("Destination download returned no readable stream for checksum verification");
    }
    const destinationSha256 = await calculateSha256(destinationStream, `${manifest.route} destination`, actualLength);
    if (destinationSha256 !== manifest.sourceSha256) {
        throw new McmaException(`Destination SHA-256 mismatch. Expected ${manifest.sourceSha256}, got ${destinationSha256}`);
    }
    log(`Verified destination SHA-256: ${destinationSha256}`);
}

function runChild(phase: TestPhase, manifestPath: string, shouldCrash: boolean, crashNumber: number): Promise<number | null> {
    return new Promise((resolve, reject) => {
        const child = fork(__filename, [
            `--phase=${phase}`,
            `--manifest=${manifestPath}`,
            `--crash=${shouldCrash}`,
            `--crash-number=${crashNumber}`,
        ], {
            env: process.env,
            stdio: "inherit",
        });
        child.once("error", reject);
        child.once("exit", code => resolve(code));
    });
}

async function runIntegrationTest() {
    initializeCloudClients();
    const routes = getRoutes();
    const testFile = await prepareTestFile();
    const suiteRunId = new Date().toISOString().replace(/[^0-9]/g, "").substring(0, 17);
    const crashCount = readIntegerSetting("FILE_COPIER_TEST_CRASHES", 3);

    log(`Starting live-checkpoint suite ${suiteRunId}`);
    log(`Routes: ${routes.join(", ")}`);
    log(`Forced terminations per route: ${crashCount}`);

    for (const route of routes) {
        const manifest = await buildManifest(route, suiteRunId, testFile);
        const manifestPath = path.join(manifest.stateDirectory, "manifest.json");
        log(`\nStarting ${manifest.route} live-checkpoint recovery test`);
        log(`State directory: ${manifest.stateDirectory}`);

        for (let crashIndex = 0; crashIndex < crashCount; crashIndex++) {
            const phase: TestPhase = crashIndex === 0 ? "capture" : "resume";
            const crashNumber = crashIndex + 1;
            const exitCode = await runChild(phase, manifestPath, true, crashNumber);
            if (exitCode !== CRASH_EXIT_CODE) {
                throw new McmaException(`${route} crash process ${crashNumber} exited with ${exitCode}; expected ${CRASH_EXIT_CODE}`);
            }
            log(`${route}: forced termination ${crashNumber}/${crashCount} completed; starting a fresh process.`);
        }

        const resumeExitCode = await runChild("resume", manifestPath, false, crashCount);
        if (resumeExitCode !== 0) {
            throw new McmaException(`${route} recovery process exited with ${resumeExitCode}`);
        }
        log(`${route} passed. Artifacts retained in ${manifest.stateDirectory}`);
    }

    log(`All ${routes.length} live-checkpoint routes passed with ${crashCount} forced terminations each.`);
}

async function main() {
    const phase = getArgument("phase") as TestPhase | undefined;
    const manifestPath = getArgument("manifest");
    if (phase || manifestPath) {
        if ((phase !== "capture" && phase !== "resume") || !manifestPath) {
            throw new McmaException("Internal phase invocation requires --phase=capture|resume and --manifest=<path>");
        }
        const shouldCrash = getArgument("crash") === "true";
        const crashNumber = Number(getArgument("crash-number") ?? "0");
        await runPhase(phase, manifestPath, shouldCrash, crashNumber);
        return;
    }

    await runIntegrationTest();
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
